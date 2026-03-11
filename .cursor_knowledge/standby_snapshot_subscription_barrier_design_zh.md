# Standby Snapshot 订阅屏障：设计文档

## 1. 背景
此前 standby 的 snapshot 同步主要靠 term 和 subscribe-id 覆盖判定，
无法严格保证 snapshot 一定在“订阅生效时刻所有活跃事务”之后。

## 2. 目标
为每个 standby 订阅 epoch 引入屏障：
- `barrier_ts = 所有本地 ccshard 的 ActiveTxMaxTs 最大值`
- 只有当 `current_ckpt_ts > barrier_ts` 时，该 epoch 的 snapshot 才允许发送

这样可保证 snapshot 至少覆盖订阅成功时刻之前的活跃事务影响。

## 3. 关键设计决策
- 屏障采样时机是主节点 **`ResetStandbySequenceId` 成功路径**，不是
  `StandbyStartFollowing`，也不是 `RequestStorageSnapshotSync`。
- 屏障键为 `(standby_node_id, standby_term)`。
- `standby_term = (primary_term << 32) | subscribe_id`。
- snapshot worker 在跑重 checkpoint 之前，先做一次轻量 ckpt-ts 探测。

## 4. 运行流程
1. standby 调用 `StandbyStartFollowing`，拿到 `subscribe_id` 和起始序列。
2. standby 调用 `ResetStandbySequenceId`。
3. primary 在所有 shard 上将该 standby 标记为 subscribed，并通过
   `ActiveTxMaxTsCc` 计算 `global_active_tx_max_ts`。
4. primary 在 `SnapshotManager` 注册屏障：
   - `subscription_barrier_[node_id][standby_term] = barrier_ts`
5. standby 调用 `RequestStorageSnapshotSync`（带 `standby_term`）。
6. primary 校验屏障存在后，将请求入 pending，并附上
   `subscription_active_tx_max_ts`。
7. `StandbySyncWorker` 周期处理：
   - 先通过 `GetCurrentCheckpointTs()` 读取当前 ckpt_ts
   - 只挑选满足以下条件的任务：
     - primary term 一致
     - `subscribe_id < current_subscribe_id`
     - `current_ckpt_ts > barrier_ts`
   - 若本轮无可发送任务，跳过重 checkpoint
   - 若有可发送任务，执行 `RunOneRoundCheckpoint`，再创建/发送 snapshot，
     并通知 standby

## 5. Pending 与清理策略
- subscribe-id 或 barrier 不满足的任务会保留在 pending 中等待后续轮次。
- 为避免空转，worker 在 pending 非空但阻塞时使用退避等待。
- 同节点新 term 注册会淘汰旧 term 的 pending/barrier。
- leader 失效时清空全部 pending 和 barrier。
- 节点移除时，按 node_id 清空对应 pending 与 barrier。

## 6. 安全性
- snapshot-sync 请求找不到 barrier 一律拒绝（安全默认）。
- 屏障按订阅 epoch 隔离，不跨 term 复用。
- pending/barrier 的所有读写都在 `standby_sync_mux_` 保护下。

## 7. 预期效果
- 避免过早 snapshot 遗漏订阅时在途事务写入。
- 在无可发送任务时避免不必要的重 checkpoint 开销。
- 保持现有 snapshot 传输和重试语义不变。
