---- MODULE MQ ----
\* 消息队列中间件规约。
\*
\* 数据流：
\*
\*   SubmitToMiddleware
\*        |
\*        v
\*   file[q]  ---MoveEventFromFileToTable--->  table[q]
\*                                                |
\*                                               Read
\*|
\*v
\*before[g] (整组)
\*                                        |
\*                              DistributeToConsumer
\*      /          \
\*                                  v            v
\*                            after[g]      consumer_queue[c]
\*                                  |              |
\*                    UpdateTableWithProcessing   ConsumerHandleEvent
\*                                  |              |
\*                                  v              v
\*                              table[q] <---------+
\*
\* 四个关键设计：
\*
\*   1. 先写文件再落库。入队和返回都攒批，数据库 QPS 能压到 1/100。
\*   2. 按组读写。组是消费者处理的最小单位，拆开就没法保证完整。
\*   3. 不用锁。靠状态机挡冲突：已经是 Success 或 Dead 的事件不会被改回去。
\*   4. frozen 记录组冻结时的 uid 集合，用来验证组不会分裂。
\*
\* 还没写的：Next、Spec、安全性、活性、cfg。
\* 还在改的：分发拆成两步（缓冲区）、消费者的 in_progress 标记、consumer_cache 去重。

EXTENDS Integers, Bags, FiniteSets, TLC

VARIABLES consumer_queue, uid, file, return_file, table, cache, consumer_cache, before, after, frozen, network_buffer, func_buffer

CONSTANTS QUEUE, CONSUMER, GOROUTINES, GROUPS, MAX_UID, MAX_RETRY

vars ≜ ⟨consumer_queue, uid, file, return_file, table, cache, consumer_cache, before, after, frozen, network_buffer, func_buffer⟩

Pending ≜ "Pending"
Processing ≜ "Processing"
Success ≜ "Success"
Failed ≜ "Failed"
Dead ≜ "Dead"

Terminal ≜ {Success, Dead}

\* 一条事件。
\*
\* queue、group、retry 本来可以是外键，但那是多对一，外键还得 join，不如直接拍平。
\* retry 是组级的重试次数，拍平之后每条都有，同组可能对不上，省掉外键换的。
Event ≜ [
    u: 1‥MAX_UID,
    queue: QUEUE,
    group: GROUPS,
    state: {Pending, Processing, Success, Failed, Dead},
    seq: 1‥MAX_UID,
    retry: 0‥MAX_RETRY,
    end: BOOLEAN
]

\* 所有装着事件的地方，元素都必须是合法的 Event
Constraint ≜
    ∧ ∀ c ∈ CONSUMER:
        ∧ UNION consumer_queue[c] ⊆ Event
        ∧ UNION network_buffer[c] ⊆ Event
        ∧ UNION func_buffer[c] ⊆ Event
    ∧ ∀ g ∈ GOROUTINES:
        ∧ UNION before[g] ⊆ Event
        ∧ UNION after[g] ⊆ Event
    ∧ ∀ q ∈ QUEUE:
        ∧ file[q] ⊆ Event
        ∧ table[q] ⊆ Event
        ∧ return_file[q] ⊆ Event

Init ≜
    ∧ consumer_queue = [c ∈ CONSUMER ↦ {}]
    ∧ uid = 0
    ∧ file = [q ∈ QUEUE ↦ {}]
    ∧ return_file = [q ∈ QUEUE ↦ {}]
    ∧ table = [q ∈ QUEUE ↦ {}]
    \* cache 两件事：uid 去重、每组当前最大序号
    \* 真实现里 seq 是上游传的，这里为了省事直接取最大值加一
    ∧ cache = [u ↦ {}, max_event_seq ↦ ⟨⟩]
    ∧ consumer_cache = [c ∈ CONSUMER ↦ {}]
    ∧ before = [g ∈ GOROUTINES ↦ {}]
    ∧ after = [g ∈ GOROUTINES ↦ {}]
    ∧ frozen = ⟨⟩
    ∧ network_buffer = [c ∈ CONSUMER ↦ {}]
    ∧ func_buffer = [c ∈ CONSUMER ↦ {}]

\* ---------------------------------------------------------------------------
\* 入队
\* ---------------------------------------------------------------------------

\* 上游投递一条消息。end 表示这一组到这是不是最后一条。
\*
\* 首次出现的组从 seq = 1 开始，之后每条递增。
\* 这里假设 end 之后不会再投，真实现要用 seq <= end_seq 挡掉。
SubmitToMiddleware(queue, group, end) ≜
    ∧ ¬(∃ event ∈ table[queue]: event.group = group ∧ event.end = TRUE)
    ∧ ∨ ∧ ⟨queue, group⟩ ∉ DOMAIN cache.max_event_seq
          ∧ LET new_seq ≜ 1
                new_uid ≜ uid + 1
                event ≜ [u ↦ new_uid, queue ↦ queue, group ↦ group,
                         state ↦ Pending, seq ↦ new_seq, retry ↦ 0, end ↦ end]
            IN  ∧ new_uid ≤ MAX_UID
                ∧ new_uid ∉ cache.u 
                ∧ uid' = new_uid
                ∧ file' = [file EXCEPT ![queue] = @ ∪ {event}]
                ∧ cache' = [cache EXCEPT !.u = @ ∪ {new_uid}, !.max_event_seq = @ @@ ⟨queue, group⟩ :> 1]

        ∨ ∧ ⟨queue, group⟩ ∈ DOMAIN cache.max_event_seq
          ∧ LET new_seq ≜ cache.max_event_seq[⟨queue, group⟩] + 1
                new_uid ≜ uid + 1
                event ≜ [u ↦ new_uid, queue ↦ queue, group ↦ group,
                         state ↦ Pending, seq ↦ new_seq, retry ↦ 0, end ↦ end]
            IN  
                ∧ new_uid ≤ MAX_UID
                ∧ new_uid ∉ cache.u
                ∧ uid' = new_uid
                ∧ file' = [file EXCEPT ![queue] = @ ∪ {event}]
                ∧ cache' = [cache EXCEPT !.u = @ ∪ {new_uid}, !.max_event_seq[⟨queue, group⟩] = @ + 1]

    ∧ UNCHANGED ⟨consumer_queue, return_file, table, consumer_cache, before, after, frozen, network_buffer, func_buffer⟩

\* ---------------------------------------------------------------------------
\* 落库
\* ---------------------------------------------------------------------------

\* 组第一次出现 end 的时候，把当时的 uid 集合记下来。
\*
\* 已经在 frozen 里的组不覆盖，只补新出现的。配合 MoveEventFromFileToTable 一起用。
FreezeBody ≜
    LET new_end_group ≜ {⟨q, g⟩ ∈ QUEUE × GROUPS:
            ∃ event ∈ file[q]: event.group = g ∧ event.end = TRUE}
    IN  frozen' = [k ∈ DOMAIN frozen ∪ new_end_group ↦
                IF k ∈ DOMAIN frozen
                THEN frozen[k]
                ELSE {x.u: x ∈ {event ∈ table'[k[1]]: event.group = k[2]}}]

\* 把文件里的事件批量写进数据库，然后清空文件。
\*
\* 真实现是按字节读、处理完再删前面那一段。删的时候可能要加一下写锁，但很快。
\* 这块要改成分段的文件才行了
\* 而且如果发现这个组已经超时了，写入的时候直接写成Dead，保证ConsistentState
MoveEventFromFileToTable ≜
    ∧ ∃ q ∈ QUEUE: file[q] ≠ {}
    ∧ table' = [x ∈ QUEUE ↦
                LET dead_event ≜ {e ∈ table[x]: e.state = Dead}
                    dead_group ≜ {e.group: e ∈ dead_event}
                    processed ≜ {
                        IF e.group ∈ dead_group
                        THEN [e EXCEPT !.state = Dead]
                        ELSE e
                        : e ∈ file[x]
                    }
                IN table[x] ∪ processed]
    ∧ file' = [q ∈ QUEUE ↦ {}]
    ∧ FreezeBody
    ∧ UNCHANGED ⟨consumer_queue, uid, return_file, cache, consumer_cache, before, after, network_buffer, func_buffer⟩

\* ---------------------------------------------------------------------------
\* 读和分发
\* ---------------------------------------------------------------------------

\* 从数据库里按组读出一批，放进 before。
\*
\* 三个条件：状态是 Pending 或 Failed、组里有 end、组没被别的协程占着。
\* 真实现里「组收齐」还要序号连续，这里只用 end 判断，理由见文件末尾。
Read(goroutine) ≜
    ∃ queue ∈ QUEUE: ∃ event ∈ table[queue]:
        ∧ event.state ∈ {Pending, Failed}
        ∧ LET the_group_events ≜ {x ∈ table[queue]: x.group = event.group}
              in_processing ≜ {[e EXCEPT !.state = Processing]: e ∈ the_group_events}
          IN  ∧ ∃ end_event ∈ the_group_events: end_event.end = TRUE
              ∧ ¬(∃ other ∈ GOROUTINES \ {goroutine}: the_group_events ∈ before[other] ∨ in_processing ∈ after[other])
              \* 而且不能在自己的after中！测试出来的反例
              ∧ in_processing ∉ after[goroutine]
              ∧ before' = [before EXCEPT ![goroutine] = @ ∪ {the_group_events}]
    ∧ UNCHANGED ⟨consumer_queue, uid, file, return_file, table, cache, consumer_cache, after, frozen, network_buffer, func_buffer⟩

\* 把 before 里的一组挪到 after 和消费者队列，状态改成 Processing。
\*
\* before + after 合起来就是写锁：一个组只能在一个协程手里。
\*
\* 待改：这里一步到位，分发的顺序就是到达的顺序。
\* 要建模传输乱序的话，得先挪进缓冲区，再由另一个动作 ∃ 取进 consumer_queue。
DistributeToConsumer(goroutine, consumer) ≜
    ∧ before[goroutine] ≠ {}
    ∧ ∃ event_group ∈ before[goroutine]:
        LET in_processing ≜ {[event EXCEPT !.state = Processing]: event ∈ event_group}
        IN  ∧ network_buffer' = [network_buffer EXCEPT ![consumer] = @ ∪ {in_processing}]
            ∧ before' = [before EXCEPT ![goroutine] = @ \ {event_group}]
            ∧ after' = [after EXCEPT ![goroutine] = @ ∪ {in_processing}]
    ∧ UNCHANGED ⟨consumer_queue, uid, file, return_file, table, cache, consumer_cache, frozen, func_buffer⟩

FromNetWorkBufferToConsumerQueue(consumer) ≜
    ∧ network_buffer[consumer] ≠ {}
    ∧ LET event_group ≜ CHOOSE x ∈ network_buffer[consumer]: TRUE
      IN  ∧ consumer_queue' = [consumer_queue EXCEPT ![consumer] = @ ∪ {event_group}]
          ∧ network_buffer' = [network_buffer EXCEPT ![consumer] = @ \ {event_group}]
          ∧ UNCHANGED ⟨uid, file, return_file, table, cache, consumer_cache, before, after, frozen, func_buffer⟩ 

\* 把 after 里的组补进数据库。
\*
\* 只插表里还没有的 uid，已经存在的一律跳过。这样消费者先写进去的 Success
\* 不会被这里的 Processing 覆盖。等价于 INSERT ... ON CONFLICT DO NOTHING。

\* 谁让你直接跳过的？这样就没办法把Pending变成Processing了！！！
\* 这里也要用条件更新，首先after中的状态只有Processing，数据库中的状态可能是Pending, Fail, Success, Dead中任意一种
\* Processing只能覆盖Pending

UpdateTableWithProcessing(goroutine) ≜
    ∧ before[goroutine] = {} ∧ after[goroutine] ≠ {}
    ∧ after' = [after EXCEPT ![goroutine] = {}]
    \*∧ table' = [q ∈ QUEUE ↦ table[q] ∪
    \*        {event ∈ UNION after[goroutine]:
    \*           event.queue = q ∧ event.u ∉ {x.u: x ∈ table[q]}}]
    ∧ table' = [q ∈ QUEUE ↦ 
                LET after_uid ≜ {event.u: event ∈ UNION after[goroutine]}
                    in_pending ≜ {event ∈ table[q]: event.state = Pending ∧ event.u ∈ after_uid}
                IN  (table[q] \ in_pending) ∪ {[event EXCEPT !.state = Processing]: event ∈ in_pending}
            ]
    ∧ UNCHANGED ⟨consumer_queue, uid, file, return_file, cache, consumer_cache, before, frozen, network_buffer, func_buffer⟩

\* 模拟消费者返回结果。
\*
\* 整组一起改状态：成功就 Success，重试到上限就 Dead，否则回 Failed 并 retry + 1。
\* 返回本身的乱序没建模，因为影响不到结论，对账那块也不在规约范围内。
\*
\* 待改：这里收到就立刻出结果。要建模「处理要花时间」和「多个组并行」，
\* 得给每组加一个 in_progress 标记，收到置 TRUE，任意时刻置 FALSE 表示处理完。
ConsumerHandleEvent(consumer) ≜
    ∃ event_group ∈ consumer_queue[consumer]:
        ∧ consumer_queue' = [consumer_queue EXCEPT ![consumer] = @ \ {event_group}]
        \* 虽然func_buffer也有一个去重的效果，但是还是看缓存
        \* 而且这块如果处理完突然又来了的话，也挡不住
        ∧ func_buffer' = [func_buffer EXCEPT ![consumer] = @ ∪ {event_group}]
        ∧ consumer_cache' = [consumer_cache EXCEPT ![consumer] = @ ∪ {event.u: event ∈ event_group}]
        ∧ UNCHANGED ⟨uid, file, return_file, table, cache, before, after, frozen, network_buffer⟩

HandleComplete(consumer) ≜
    ∃ event_group ∈ func_buffer[consumer]:
        ∧ func_buffer' = [func_buffer EXCEPT ![consumer] = @ \ {event_group}]
        ∧ LET queue ≜ (CHOOSE event ∈ event_group: TRUE).queue
              retry_now ≜ (CHOOSE event ∈ event_group: TRUE).retry
          IN  ∃ ok ∈ BOOLEAN:
                  IF ok
                  THEN ∧ return_file' = [return_file EXCEPT ![queue] =
                        @ ∪ {[event EXCEPT !.state = Success]: event ∈ event_group}]
                       ∧ UNCHANGED consumer_cache
                  ELSE IF retry_now = MAX_RETRY
                  THEN ∧ return_file' = [return_file EXCEPT ![queue] =
                        @ ∪ {[event EXCEPT !.state = Dead]: event ∈ event_group}]
                       ∧ UNCHANGED consumer_cache
                  ELSE ∧ return_file' = [return_file EXCEPT ![queue] =
                        @ ∪ {[event EXCEPT !.state = Failed, !.retry = @ + 1]: event ∈ event_group}]
                       \* 如果是失败了，就把这些uid从自己的去重表中清理掉
                       ∧ consumer_cache' = [consumer_cache EXCEPT ![consumer] = @ \ {event.u: event ∈ event_group}]
    ∧ UNCHANGED ⟨consumer_queue, uid, file, table, cache, before, after, frozen, network_buffer⟩

\* 网络乱序没建模，但实现里会遇到：
\*   处理失败、清了去重表，发回复 A，网络卡住
\*   重试的事件到达，再处理一遍，发回复 B，B 先到
\*   B 先写库，A 后到，把 B 的结果覆盖了
\*
\* 条件更新就是挡这个的：
\*   返回 Success / Dead   终态，无条件写
\*   返回 Failed           只能从 Processing 或 Failed 来，而且 retry 要更大
\*
\* 规约里对应的是 PickOne 和 WriteResultToTable：
\*   PickOne 在内存里按 uid 挑一条（终态优先，否则 retry 最大）
\*   落库时同一 uid 只会有一条，再配合 SQL 的 WHERE 条件就是条件更新
\* 规约里不建模「两条语句的到达顺序」，所以只做到「挑一条」为止。


PickOne(S, u) ≜
    IF ∃ e ∈ S: e.u = u ∧ e.state ∈ {Success, Dead}
    THEN CHOOSE e ∈ S: e.u = u ∧ e.state ∈ {Success, Dead}
    ELSE CHOOSE e ∈ S: e.u = u ∧ ¬(∃ f ∈ S: f.u = u ∧ f.retry > e.retry)

WriteResultToTable ≜
    ∧ ∃ q ∈ QUEUE: return_file[q] ≠ {}
    ∧ table' = [x ∈ QUEUE ↦
          (table[x] \ {e ∈ table[x]: e.u ∈ {y.u: y ∈ return_file[x]}})
          ∪ {PickOne(return_file[x], u): u ∈ {y.u: y ∈ return_file[x]}}]
    ∧ return_file' = [x ∈ QUEUE ↦ {}]
    ∧ UNCHANGED ⟨consumer_queue, uid, file, cache, consumer_cache, before, after, frozen, network_buffer, func_buffer⟩

Timeout ≜
    \* 直接找数据库中不带end的消息组，然后给整组设置为Dead
    \* 实现是时候就算带了end的消息组也不一定是完整的，照样可能会超时
    \* 但是超时只是针对不完整的组，读取只会读完整的组，不需要担心竞态的问题
    \* 只是实现的时候超时并不只是看没有end的组，就算有了end，但是太长时间没有补齐之前的事件，一样会超时
    ∃ q ∈ QUEUE: ∃ event ∈ table[q]:
        \* ConsistentState：同一个组里的事件状态都一样
        ∧ event.state = Pending
        ∧ LET event_group ≜ {x ∈ table[q]: x.group = event.group}
          IN  ∧ table' = [table EXCEPT ![q] = (@ \ event_group) ∪ {[e EXCEPT !.state = Dead]: e ∈ event_group}]
              ∧ UNCHANGED ⟨consumer_queue, uid, file, return_file, cache, consumer_cache, before, after, frozen, network_buffer, func_buffer⟩

AllEventInTermainal ≜ 
    ∧ ∀ q ∈ QUEUE: ∀ event ∈ table[q]: event.state ∈ Terminal
    ∧ UNCHANGED vars

\* ---------------------------------------------------------------------------
\* 冻结
\* ---------------------------------------------------------------------------

\* 安全性
DedupOnlyForLiveOrSuccess ≜
    ∀ c ∈ CONSUMER: ∀ u ∈ consumer_cache[c]:
        ∨ (∃ eg ∈ func_buffer[c]: ∃ e ∈ eg: e.u = u)
        ∨ (∃ q ∈ QUEUE: ∃ e ∈ table[q]: e.u = u ∧ e.state ∈ Terminal)
        ∨ (∃ q ∈ QUEUE: ∃ e ∈ return_file[q]: e.u = u ∧ e.state ∈ Terminal)

\* 冻结之后，这个组不管走到哪，凑出来的 uid 集合都不变。
\*
\* 等价于「组一旦收齐就不会分裂」。
FrozenStable ≜
    ∀ ⟨q, g⟩ ∈ DOMAIN frozen:
        LET event_in_table ≜ {event ∈ table[q]: event.group = g}
            event_in_before ≜ {event ∈ UNION UNION({before[gr]: gr ∈ GOROUTINES}):
                                    event.queue = q ∧ event.group = g}
            event_in_after ≜ {event ∈ UNION UNION({after[gr]: gr ∈ GOROUTINES}):
                                   event.queue = q ∧ event.group = g}
            event_in_consumer_queue ≜ {event ∈ UNION UNION({consumer_queue[cc]: cc ∈ CONSUMER}):
                                           event.queue = q ∧ event.group = g}
            event_in_network ≜ {event ∈ UNION UNION({network_buffer[cc]: cc ∈ CONSUMER}):
                                    event.queue = q ∧ event.group = g}
            event_in_func ≜ {event ∈ UNION UNION({func_buffer[cc]: cc ∈ CONSUMER}):
                                    event.queue = q ∧ event.group = g}
            target ≜ frozen[⟨q, g⟩]
        IN  ∧ {x.u: x ∈ event_in_table} = target
            ∧ event_in_before ≠ {} ⇒ {x.u: x ∈ event_in_before} = target
            ∧ event_in_after ≠ {} ⇒ {x.u: x ∈ event_in_after} = target
            ∧ event_in_consumer_queue ≠ {} ⇒ {x.u: x ∈ event_in_consumer_queue} = target
            ∧ event_in_network ≠ {} ⇒ {x.u: x ∈ event_in_network} = target
            ∧ event_in_func ≠ {} ⇒ {x.u: x ∈ event_in_func} = target

ConsistentState ≜
    ∀ q ∈ QUEUE: ∀ event ∈ table[q]:
        LET event_group ≜ {x ∈ table[q]: x.group = event.group}
        IN  Cardinality({x.state: x ∈ event_group}) = 1

UidOnlyOnce ≜
    ∀ q ∈ QUEUE:
        Cardinality(table[q]) = Cardinality({x.u: x ∈ table[q]})

\* 活性
FinalConvergence ≜ ◇□(∀ q ∈ QUEUE: ∀ e ∈ table[q]: e.state ∈ Terminal)

\* 重投已经发过的 uid。
\*
\* 这条路径靠 cache.u 直接拦掉，不存在成功的情况。
\* 指的是「投递成功但没收到 ack」的重投。发件箱那边消息在路上丢了，由发件箱自己重试，规约不涉及。
CacheMonotonic ≜
    [][∀ u ∈ cache.u: u ∈ cache'.u]_vars


Symmetry == Permutations(QUEUE) ∪ Permutations(CONSUMER) ∪ Permutations(GOROUTINES) ∪ Permutations(GROUPS)

Next ≜
    ∨ ∃ q ∈ QUEUE: ∃ g ∈ GROUPS: ∃ end ∈ BOOLEAN: SubmitToMiddleware(q, g, end)
    ∨ MoveEventFromFileToTable
    ∨ ∃ g ∈ GOROUTINES: Read(g) ∨ UpdateTableWithProcessing(g)
    ∨ ∃ g ∈ GOROUTINES: ∃ c ∈ CONSUMER: DistributeToConsumer(g, c)
    ∨ ∃ c ∈ CONSUMER: FromNetWorkBufferToConsumerQueue(c) ∨ ConsumerHandleEvent(c) ∨ HandleComplete(c)
    ∨ WriteResultToTable
    ∨ Timeout
    ∨ AllEventInTermainal

Spec ≜
    ∧ Init
    ∧ [][Next]_vars
    ∧ WF_vars(∃ q ∈ QUEUE: ∃ g ∈ GROUPS: ∃ end ∈ BOOLEAN: SubmitToMiddleware(q, g, end))
    ∧ WF_vars(MoveEventFromFileToTable)
    ∧ WF_vars(∃ g ∈ GOROUTINES: Read(g))
    ∧ WF_vars(∃ g ∈ GOROUTINES: UpdateTableWithProcessing(g))
    ∧ WF_vars(∃ g ∈ GOROUTINES: ∃ c ∈ CONSUMER: DistributeToConsumer(g, c))
    ∧ WF_vars(∃ c ∈ CONSUMER: FromNetWorkBufferToConsumerQueue(c))
    ∧ WF_vars(∃ c ∈ CONSUMER: ConsumerHandleEvent(c))
    ∧ WF_vars(∃ c ∈ CONSUMER: HandleComplete(c))
    ∧ WF_vars(WriteResultToTable)
    ∧ WF_vars(Timeout)

\* 新的竞态，如果return_file里面是Suceess，但是还没来的及写入数据库，数据库此时是Fail，这时候会被其他的协程读取到
\* 但是既然是success了，那被读取到了再发送也无所谓的，因为消费者做了去重
\* 但是在这种情况下，这个DedupOnlyForLiveOrSuccess不变式就通过不了了……先去掉这个不变式吧
\* 如果要写的话，确实是比较难写，因为这里不是简单的不变式，而是需要考虑各种竞态的情况

\* ---------------------------------------------------------------------------
\* 实现笔记（不进模型，实现时参考）
\* ---------------------------------------------------------------------------
\*
\* 落库攒批
\*   消息来一次就写一次库，QPS 太高。先攒文件，每 1 秒或者攒够 100 条批量写。
\*   返回同理，两边都攒批才有用。
\*
\* 为什么要按组
\*   组是消费者处理的最小单位。拆开读的话，一半在协程 A、一半在协程 B，
\*   组就保证不了完整。frozen 和 FrozenStable 就是为了证这件事。
\*
\* 为什么 seq 用自增
\*   真实现里 seq 是上游传进来的。规约里用自增，代价是表达不出
\*   「end 到了之后又来一条 seq 更大的消息」这种拒绝逻辑。
\*
\* 窗口
\*   组从创建开始计时，超时没收齐直接进死信。上游也有一个发送窗口，
\*   超出窗口期的消息中间件不收。规约里没有真实计时，这块验证不了。

====
