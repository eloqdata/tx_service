/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <brpc/channel.h>
#include <bthread/condition_variable.h>

#include "proto/cc_request.pb.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
/**
 * @brief CcNodeService is a RPC service associated with each physical node,
 * providing sync RPCs invoked by remote nodes. It is on the same port as the
 * node's cc stream service, which accepts a stream of cc requests from remote
 * nodes.
 *
 */
class CcNodeService : public CcRpcService
{
public:
    CcNodeService(LocalCcShards &local_shards);

    void OnLeaderStart(::google::protobuf::RpcController *controller,
                       const OnLeaderStartRequest *request,
                       OnLeaderStartResponse *response,
                       ::google::protobuf::Closure *done) override;

    void OnLeaderStop(::google::protobuf::RpcController *controller,
                      const OnLeaderStopRequest *request,
                      OnLeaderChangeResponse *response,
                      ::google::protobuf::Closure *done) override;

    void OnStartFollowing(::google::protobuf::RpcController *controller,
                          const OnStartFollowingRequest *request,
                          OnLeaderChangeResponse *response,
                          ::google::protobuf::Closure *done) override;

    void CheckTxStatus(::google::protobuf::RpcController *controller,
                       const CheckTxStatusRequest *request,
                       CheckTxStatusResponse *response,
                       ::google::protobuf::Closure *done) override;

    void NotifyNewLeaderStart(::google::protobuf::RpcController *controller,
                              const NotifyNewLeaderStartRequest *request,
                              NotifyNewLeaderStartResponse *response,
                              ::google::protobuf::Closure *done) override;
    void GetMinTxStartTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GetMinTxStartTsRequest *request,
        ::txservice::remote::GetMinTxStartTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void ClusterAddNodeGroup(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterAddNodeGroupRequest *request,
        ::txservice::remote::ClusterScaleResponse *response,
        ::google::protobuf::Closure *done) override;

    void ClusterRemoveNode(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterRemoveNodeRequest *request,
        ::txservice::remote::ClusterScaleResponse *response,
        ::google::protobuf::Closure *done) override;

    void NodeGroupAddPeers(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::NodeGroupAddPeersRequest *request,
        ::txservice::remote::ClusterScaleResponse *response,
        ::google::protobuf::Closure *done) override;

    void GetClusterNodes(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GetClusterNodesRequest *request,
        ::txservice::remote::GetClusterNodesResponse *response,
        ::google::protobuf::Closure *done) override;

    void CheckClusterScaleStatus(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterScaleStatusRequest *request,
        ::txservice::remote::ClusterScaleStatusResponse *response,
        ::google::protobuf::Closure *done) override;

    void FlushDataAll(::google::protobuf::RpcController *controller,
                      const FlushDataAllRequest *request,
                      FlushDataAllResponse *response,
                      ::google::protobuf::Closure *done) override;

    void InitDataMigration(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::InitMigrationRequest *request,
        ::txservice::remote::InitMigrationResponse *response,
        ::google::protobuf::Closure *done) override;

    void GenerateSkFromPk(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GenerateSkFromPkRequest *request,
        ::txservice::remote::GenerateSkFromPkResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadBatch(::google::protobuf::RpcController *controller,
                     const ::txservice::remote::UploadBatchRequest *request,
                     ::txservice::remote::UploadBatchResponse *response,
                     ::google::protobuf::Closure *done) override;

    void PublishBucketsMigrating(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::PubBucketsMigratingRequest *request,
        ::txservice::remote::PubBucketsMigratingResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadRangeSlices(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UploadRangeSlicesRequest *request,
        ::txservice::remote::UploadRangeSlicesResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadBatchSlices(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UploadBatchSlicesRequest *request,
        ::txservice::remote::UploadBatchResponse *response,
        ::google::protobuf::Closure *done) override;

    void FetchPayload(::google::protobuf::RpcController *controller,
                      const ::txservice::remote::FetchPayloadRequest *request,
                      ::txservice::remote::FetchPayloadResponse *response,
                      ::google::protobuf::Closure *done) override;

    void FetchCatalog(::google::protobuf::RpcController *controller,
                      const ::txservice::remote::FetchPayloadRequest *request,
                      ::txservice::remote::FetchPayloadResponse *response,
                      ::google::protobuf::Closure *done) override;

    void StandbyStartFollowing(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::StandbyStartFollowingRequest *request,
        ::txservice::remote::StandbyStartFollowingResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateStandbyCkptTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UpdateStandbyCkptTsRequest *request,
        ::txservice::remote::UpdateStandbyCkptTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateStandbyConsistentTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UpdateStandbyConsistentTsRequest *request,
        ::txservice::remote::UpdateStandbyConsistentTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void RequestStorageSnapshotSync(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::StorageSnapshotSyncRequest *request,
        ::txservice::remote::StorageSnapshotSyncResponse *response,
        ::google::protobuf::Closure *done) override;

    void OnSnapshotSynced(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::OnSnapshotSyncedRequest *request,
        ::txservice::remote::OnSnapshotSyncedResponse *response,
        ::google::protobuf::Closure *done) override;

    void FetchNodeInfo(::google::protobuf::RpcController *controller,
                       const ::txservice::remote::FetchNodeInfoRequest *request,
                       ::txservice::remote::FetchNodeInfoResponse *response,
                       ::google::protobuf::Closure *done) override;

    void ResetStandbySequenceId(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ResetStandbySequenceIdRequest *request,
        ::txservice::remote::ResetStandbySequenceIdResponse *response,
        ::google::protobuf::Closure *done) override;

    void CreateBackup(::google::protobuf::RpcController *controller,
                      const ::txservice::remote::CreateBackupRequest *request,
                      ::txservice::remote::FetchBackupResponse *response,
                      ::google::protobuf::Closure *done) override;

    void FetchBackup(::google::protobuf::RpcController *controller,
                     const ::txservice::remote::FetchBackupRequest *request,
                     ::txservice::remote::FetchBackupResponse *response,
                     ::google::protobuf::Closure *done) override;

    void TerminateBackup(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::TerminateBackupRequest *request,
        ::txservice::remote::TerminateBackupResponse *response,
        ::google::protobuf::Closure *done) override;

    void CreateClusterBackup(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::CreateClusterBackupRequest *,
        ::txservice::remote::ClusterBackupResponse *,
        ::google::protobuf::Closure *done) override;

    void FetchClusterBackup(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::FetchClusterBackupRequest *,
        ::txservice::remote::ClusterBackupResponse *,
        ::google::protobuf::Closure *done) override;

    void NotifyShutdownCkpt(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::NotifyShutdownCkptRequest *request,
        ::txservice::remote::NotifyShutdownCkptResponse *response,
        ::google::protobuf::Closure *done) override;

    void CheckCkptStatus(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::CheckCkptStatusRequest *request,
        ::txservice::remote::CheckCkptStatusResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateClusterConfig(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UpdateClusterConfigRequest *request,
        ::google::protobuf::Empty *response,
        ::google::protobuf::Closure *done) override;

private:
    LocalCcShards &local_shards_;
};
}  // namespace remote
}  // namespace txservice
