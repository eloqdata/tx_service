# Standby Snapshot 订阅屏障：实现文档

## 1. 代码改动范围
- `tx_service/src/remote/cc_node_service.cpp`
- `tx_service/include/store/snapshot_manager.h`
- `tx_service/src/store/snapshot_manager.cpp`
- `tx_service/include/cc/cc_shard.h`（若缺少 `ActiveTxMaxTs`）
- 可选：新增跨 shard 聚合请求类

## 2. 新增/调整状态

### 2.1 `SnapshotManager` 屏障表
新增按 standby node + standby term 索引的屏障映射：
- `subscription_barrier_[standby_node_id][standby_term] = barrier_ts`

### 2.2 pending 任务结构扩展
将 pending value 从单一 request 扩展为任务结构，至少包括：
- `req: StorageSnapshotSyncRequest`
- `subscription_active_tx_max_ts`
- 可选 `created_at`

## 3. `SnapshotManager` 新接口
- `RegisterSubscriptionBarrier(ng_id, standby_node_id, standby_term, barrier_ts)`
- `GetSubscriptionBarrier(standby_node_id, standby_term, uint64_t* out)`
- `EraseSubscriptionBarrier(standby_node_id, standby_term)`
- 可选：清理旧 term 的辅助方法

以上接口及 pending 操作统一受 `standby_sync_mux_` 保护。

## 4. `StandbyStartFollowing` 中采样并注册
在 primary 侧 `CcNodeService::StandbyStartFollowing` 中：
1. term 校验通过后；
2. 计算 `global_max_active_tx_ts = max(shard.ActiveTxMaxTs(ng_id))`；
3. 生成 `subscribe_id`；
4. 组合 `standby_term`；
5. 注册屏障到 `SnapshotManager`。

实现注意：
- `ActiveTxMaxTs` 聚合应在 shard 安全上下文执行（参考现有跨 shard 请求模式）。

## 5. `RequestStorageSnapshotSync` 路径调整
在 `SnapshotManager::OnSnapshotSyncRequested` 中：
1. 取 `standby_node_id + standby_node_term`；
2. 查询 barrier；
3. 查询失败则拒绝入队；
4. 查询成功则将 barrier 附加到 pending 任务。

去重语义保持 term 优先；任务被新 term 覆盖时同步替换 barrier。

## 6. `SyncWithStandby` 判定扩展
保留现有判定，新增 barrier 条件：
- 取本轮 `ckpt_ts`；
- 要求 `ckpt_ts > task.subscription_active_tx_max_ts`。

不满足则任务保留到下一轮。

## 7. `RunOneRoundCheckpoint` 输出 ckpt_ts
当前只返回 bool，建议改为：
- `bool RunOneRoundCheckpoint(uint32_t node_group, int64_t term, uint64_t *out_ckpt_ts)`

并在函数内将本轮 `CkptTsCc::GetCkptTs()` 写入 `out_ckpt_ts`。

## 8. `ActiveTxMaxTs` 方法
若当前无此方法，在 `CcShard` 中新增：
- `uint64_t ActiveTxMaxTs(NodeGroupId cc_ng_id)`

建议规则：
- 遍历 `lock_holding_txs_[cc_ng_id]`
- 与 min-ts 逻辑同口径（仅非 meta / 写锁相关）
- 返回该 shard 的活动事务最大时间戳

## 9. 清理策略
- 同一 `(node_id, standby_term)` snapshot 成功后删除 barrier；
- 同节点新 term 注册时删除旧 term barrier；
- standby 重置/取消订阅时清理该节点 barrier；
- 可选 TTL 扫描兜底。

## 10. 失败处理
- 请求到达但 barrier 缺失：拒绝（安全优先）；
- checkpoint 失败：任务继续排队等待下一轮；
- snapshot 发送/回调失败：沿用既有重试机制。

## 11. 测试建议

### 单测
- barrier 注册/查询/删除
- pending 去重与 barrier 替换
- 判定边界：`ckpt_ts == / < / > barrier_ts`

### 集成
- 长事务阻塞场景：checkpoint 超过 barrier 前不发送 snapshot
- 多 standby 独立 barrier
- 同 standby term 重试请求
- term 切换后的清理与隔离
