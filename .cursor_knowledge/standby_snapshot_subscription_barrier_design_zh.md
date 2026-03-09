# Standby Snapshot 订阅屏障：设计文档

## 1. 背景
当前 standby 在订阅 primary 后再触发 snapshot 同步。
现有 snapshot 可发送判定主要是 term 与 subscribe-id 覆盖。

该机制可能出现一个问题：
订阅时刻已经活跃的事务，其修改不一定全部被当轮 snapshot 覆盖。

## 2. 目标
引入“**订阅时刻活动事务屏障**”：
- 在订阅开始时，于主节点记录 `global_max_active_tx_ts`；
- 该订阅 epoch 的 snapshot 仅在满足
  - `ckpt_ts > subscription_time_global_max_active_tx_ts`
  时才有效。

这样可保证 snapshot 位于“订阅时刻所有活跃事务”之后。

## 3. 非目标
- 不改 standby forward 流协议。
- 不改事务执行语义。
- 不重写 checkpoint 算法。

## 4. 屏障采样时机
应在主节点 **`StandbyStartFollowing`** RPC 中采样，
不应在 `RequestStorageSnapshotSync` 里采样。

原因：
- `StandbyStartFollowing` 才是订阅开始时刻；
- `RequestStorageSnapshotSync` 可能延迟/重试，语义不等价于订阅瞬间。

## 5. 设计模型
每个订阅 epoch（由 `standby_term` 标识）绑定一个固定屏障值：
- `standby_term = (primary_term << 32) | subscribe_id`
- `barrier_ts = 订阅时所有本地 shard 的 ActiveTxMaxTs 最大值`

只有当 checkpoint 前进超过该屏障，才允许发送该 epoch 的 snapshot。

## 6. 运行流程
1. standby 调用 `StandbyStartFollowing`。
2. primary 计算 `barrier_ts`，分配 `subscribe_id`，组成 `standby_term`。
3. primary 保存 `(standby_node_id, standby_term) -> barrier_ts`。
4. standby 调用 `RequestStorageSnapshotSync`（携带 `standby_term`）。
5. primary 查到 barrier，并附着到 pending snapshot 任务。
6. snapshot worker 在现有判定外新增：
   - `ckpt_ts > barrier_ts`
7. 若不满足则继续等待下一轮 checkpoint；满足才发送 snapshot。

## 7. 安全与生命周期原则
- 收到 snapshot-sync 请求但找不到 barrier，按无效请求处理。
- barrier 作用域是订阅 epoch，完成后或 term 超前后要清理。
- 并发控制沿用 `SnapshotManager` 既有 standby-sync 互斥边界。

## 8. 预期效果
- 避免“过早 snapshot”遗漏订阅时活跃事务写入。
- 不改变现有重试和传输机制。
- 长事务场景下会引入可预期的等待。

## 9. 可观测性（设计层）
建议增加：
- 阻塞日志：`ckpt_ts <= barrier_ts`，包含 node/term/barrier/ckpt
- 指标：阻塞轮次、barrier map 大小
