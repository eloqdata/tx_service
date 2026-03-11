# Standby Snapshot 订阅屏障：实现文档

## 1. 代码改动范围
- `tx_service/src/remote/cc_node_service.cpp`
- `tx_service/src/fault/cc_node.cpp`
- `tx_service/include/cc/cc_request.h`（`ActiveTxMaxTsCc`）
- `tx_service/include/store/snapshot_manager.h`
- `tx_service/src/store/snapshot_manager.cpp`

## 2. 新增/调整状态

### 2.1 `SnapshotManager` 屏障表
新增按 standby node + standby term 索引的屏障映射：
- `subscription_barrier_[standby_node_id][standby_term] = barrier_ts`

### 2.2 pending 任务结构扩展
pending value 为任务结构，包含：
- `req: StorageSnapshotSyncRequest`
- `subscription_active_tx_max_ts`

## 3. `SnapshotManager` 新接口
- `RegisterSubscriptionBarrier(standby_node_id, standby_term, barrier_ts)`
- `GetSubscriptionBarrier(standby_node_id, standby_term, uint64_t* out)`
- `EraseSubscriptionBarrier(standby_node_id, standby_term)`
- `EraseSubscriptionBarriersByNode(standby_node_id)`
- `GetCurrentCheckpointTs(node_group) -> uint64_t`
- `RunOneRoundCheckpoint(node_group, leader_term) -> bool`

以上 barrier/pending 相关操作统一受 `standby_sync_mux_` 保护。

## 4. 在 `ResetStandbySequenceId` 中采样并注册
在 primary 侧 `CcNodeService::ResetStandbySequenceId` 中：
1. 先在各 shard 将 standby 从 candidate 切换为 subscribed；
2. 做 term 校验；
3. 若 `(node_id, standby_term)` 尚无 barrier：
   - 通过 `ActiveTxMaxTsCc` 聚合所有 shard 的活动事务上界
   - 调用 `SnapshotManager::RegisterSubscriptionBarrier(...)` 注册。

该时机对应“订阅真正成功”。

## 5. `RequestStorageSnapshotSync` 路径调整
在 `SnapshotManager::OnSnapshotSyncRequested` 中：
1. 取 `standby_node_id + standby_node_term`；
2. 查询 barrier；
3. 查询失败则拒绝请求；
4. 查询成功则将 barrier 附加到 pending 任务。

去重语义保持“同节点按 term 优先”。

## 6. `SyncWithStandby` 判定扩展
`SnapshotManager::SyncWithStandby` 采用两阶段：
1. 轻量阶段：
   - `current_ckpt_ts = GetCurrentCheckpointTs(node_group)`
   - 筛选满足以下条件的任务：
     - term 对齐
     - `subscribe_id < current_subscribe_id`
     - `current_ckpt_ts > subscription_active_tx_max_ts`
   - 若无可发送任务，直接返回
2. 重量阶段：
   - 执行 `RunOneRoundCheckpoint(...)` 做 flush
   - 对可发送任务创建/发送 snapshot，并通知 standby。

## 7. Worker 重试节流
`StandbySyncWorker` 在 pending 非空但任务仍阻塞时，增加短暂退避等待
（`200ms`），避免紧循环。

## 8. 清理策略
- 同一 `(node_id, standby_term)` snapshot 成功后删除 barrier；
- 同节点新 term 注册时，清理旧 term barrier，并可淘汰旧 pending；
- 节点移除时通过 `EraseSubscriptionBarriersByNode(node_id)` 同时清理
  pending 与 barrier；
- leader 失效时清空全部 pending/barrier。

## 9. 失败处理
- 请求到达但 barrier 缺失：拒绝（安全优先）；
- checkpoint 失败：任务保留，后续轮次重试；
- snapshot 发送失败：任务保留，后续轮次重试。

## 10. Standby 侧 reset 拒绝回滚
在 `CcNode::SubscribePrimaryNode` 中，如果 primary 拒绝
`ResetStandbySequenceId`：
- 回滚本地 per-shard standby 订阅状态（`Unsubscribe`）
- 若仍是失败 term，则清理 `standby/candidate standby term`
- 并发保护避免误伤更新的重订阅流程。

## 11. 测试建议

### 单测
- barrier 注册/查询/删除
- pending 去重与 barrier 替换
- 判定边界：`current_ckpt_ts == / < / > barrier_ts`

### 集成
- 长事务阻塞场景：`current_ckpt_ts` 未超过 barrier 前不发送 snapshot
- 多 standby 独立 barrier
- 同 standby term 重试请求
- term 切换后的清理与隔离
