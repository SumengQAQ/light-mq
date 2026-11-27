# ADR-0001 落库前按 uid 挑一条

## 背景

`HandleComplete` 把处理结果追加进 `return_file`。同一个组如果失败重试过，就会有两批：

```
失败那次   { Failed, Failed, Failed }
成功那次   { Success, Success, Success }
```

`WriteResultToTable` 原本是「先删表里同 uid 的旧记录，再把 `return_file` 整个并进去」。并完之后：

```
uid=1  Failed(retry=1)
uid=1  Success
```

同一个 uid 在表里出现两次。

## 问题

**安全性不好表达。**

「一组只有一种状态」这条不变式直接违反，因为表里同时有 `Failed` 和 `Success`。想绕过去就得写「同一组至多一条终态」之类的弱化版本，然后每一条新的不变式都要考虑「同一 uid 多条」的情况，越写越绕。

**和现实偏差太大。**

实现里落库是逐条 `INSERT ... ON CONFLICT (uid) DO UPDATE`，表里永远只有一行，最后写的那条生效。规约里出现两条，纯粹是因为集合没有唯一键这个概念。

这种偏差会一直传导：规约里证明的东西，和代码里跑的东西，中间隔着一层「多条怎么合并」的假设。

## 决定

在落库之前，先在内存里按 uid 挑一条。

```tla
PickOne(S, u) ≜
    IF ∃ e ∈ S: e.u = u ∧ e.state ∈ {Success, Dead}
    THEN CHOOSE e ∈ S: e.u = u ∧ e.state ∈ {Success, Dead}
    ELSE CHOOSE e ∈ S: e.u = u ∧ ¬(∃ f ∈ S: f.u = u ∧ f.retry > e.retry)
```

规则两条：

- 有终态就挑终态
- 没有终态就挑 `retry` 最大的

`WriteResultToTable` 先用 `PickOne` 聚合，再删旧的、并新的，表里保证一个 uid 一行。

## 代价和收益

**代价**：现实里其实不需要挑。`ON CONFLICT DO UPDATE` 天然只有一行，后写的覆盖先写的。规约里加 `PickOne` 是为了把「唯一」这件事显式写出来，不依赖数据库的行为。

**收益**：

- `UidOnlyOnce` 能写成一条短的不变式
- `ConsistentState` 能成立
- 挑完之后可以按状态和 `retry` 分组，同一组的一起 `UPDATE`，减少网络往返

第三条是意外的收获。本来只是为了让规约好写，结果实现上也顺了。

## 备选方案

**让 `return_file` 覆盖写，不追加。**

但文件是追加写的 WAL，覆盖意味着扫全文件找旧记录，代价更高，而且破坏了 WAL 的顺序追加语义。

**接受多条，只证「每个 uid 至多一条终态」。**

结论更弱。而且这是一个「规约妥协」，后面每一条不变式都要带着这个前提，写起来更累。

## 结论

挑。

规约里显式挑，实现里靠唯一键兜底。两边都保证「一个 uid 一行」，这样规约和代码描述的是同一件事。
