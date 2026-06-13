#pragma once
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "tx_service.h"

namespace txservice
{
namespace test
{
// Parameters for one node in a multi-node test cluster.
struct TxNodeConfig
{
    uint32_t node_id{0};
    uint32_t native_ng_id{0};
    uint32_t core_num{2};
    // ng_id -> members, each (node_id, host, base_port).
    std::map<uint32_t, std::vector<std::tuple<uint32_t, std::string, uint16_t>>>
        ng_members;
    std::string hm_ip;  // scripted host manager (the test driver)
    uint16_t hm_port{0};
    std::string data_dir;  // unique per node
};

// Brings up a real multi-node TxService for this node (mock catalog, in-process
// MemDataStore, skip_wal=true, skip_kv=true), pointed at an EXTERNAL host
// manager (fork_host_manager=false). Does NOT wait for cluster readiness; the
// driver drives OnLeaderStart and waits separately.
class TxNode
{
public:
    explicit TxNode(const TxNodeConfig &cfg);
    ~TxNode();
    TxService *Service();
    const TableName &Table() const;
    uint64_t SchemaVersion() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace test
}  // namespace txservice
