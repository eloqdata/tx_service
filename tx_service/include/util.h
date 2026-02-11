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

#if defined(WITH_JEMALLOC)
#include <jemalloc/jemalloc.h>
#endif

#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>  // For __rdtsc()
#endif

#include "sharder.h"
#include "type.h"

namespace txservice
{
/** @brief
  Split a string_view by a delimiter
 */
static inline std::vector<std::string_view> SplitStringView(
    std::string_view str, std::string_view delimter)
{
    assert(delimter.size() > 0);

    std::vector<std::string_view> output;
    size_t first = 0, last = 0;
    std::string_view token;

    while ((last = str.find(delimter, first)) != std::string::npos)
    {
        if (last > first)
        {
            token = str.substr(first, last - first);
            output.emplace_back(token);
        }

        first = last + delimter.size();

        if (first >= str.size())
        {
            break;
        }
    }

    if (first != 0 && first < str.size())
    {
        token = str.substr(first);
        output.emplace_back(token);
    }

    return output;
}

/** @brief
  Replace the 1st occurrence of to_replace with replace in str

  Output: is the to_replace found in str
 */
static inline bool ReplaceInString(std::string &str,
                                   const std::string to_replace,
                                   const std::string replace)
{
    size_t pos = str.find(to_replace);
    bool found = pos != std::string::npos;

    // Replace this occurrence of Sub String
    if (found)
    {
        str.replace(pos, to_replace.size(), replace);
    }

    return found;
}

/** @brief
  Replace all occurrences of to_replace with replace in str

  Output: is the to_replace found in str
 */
static inline bool ReplaceAllInString(std::string &str,
                                      const std::string to_replace,
                                      const std::string replace)
{
    // Get the first occurrence
    size_t pos = str.find(to_replace);
    bool found = pos != std::string::npos;
    // Repeat till end is reached
    while (pos != std::string::npos)
    {
        // Replace this occurrence of Sub String
        str.replace(pos, to_replace.size(), replace);
        // Get the next occurrence from the current position
        pos = str.find(to_replace, pos + replace.size());
    }

    return found;
}

/**
 * Merge multiple sorted ascending vectors into a single one.
 * Note that the passed in compare func need to be greater than.
 */
template <typename T, class Compare>
static inline void MergeSortedVectors(std::vector<std::vector<T>> &&vecs,
                                      std::vector<T> &output,
                                      Compare greater,
                                      bool dedup = false)
{
    // We need to build a priority queue with pair elements. Each element
    // will contain which subvec the element comes from and the actual value T.
    // Build a new cmp function for the pair object with the passed in cmp.
    auto greater_pair = [greater](std::pair<T, size_t> &p1,
                                  std::pair<T, size_t> &p2) -> bool
    { return greater(p1.first, p2.first); };
    std::priority_queue<std::pair<T, size_t>,
                        std::vector<std::pair<T, size_t>>,
                        decltype(greater_pair)>
        pq(greater_pair);
    size_t total_size = 0;
    // Record pos in each sub vec.
    std::vector<size_t> idxs;
    for (size_t i = 0; i < vecs.size(); ++i)
    {
        total_size += vecs.at(i).size();
        idxs.push_back(1);
        if (!vecs.at(i).empty())
        {
            pq.emplace(std::move(vecs.at(i).front()), i);
        }
    }
    output.reserve(total_size);
    while (pq.size())
    {
        // Move the top object to output vec before popping it.
        const std::pair<T, size_t> &top = pq.top();
        if (!dedup || output.empty() || greater(output.back(), top.first) ||
            greater(top.first, output.back()))
        {
            output.emplace_back(std::move(const_cast<T &>(top.first)));
        }
        size_t grp = top.second;
        pq.pop();
        // Add the next object from the same sub vec if it has not
        // reached the end.
        if (idxs.at(grp) < vecs.at(grp).size())
        {
            T &next = vecs.at(grp).at(idxs.at(grp));
            pq.emplace(std::move(next), grp);
            idxs.at(grp)++;
        }
    }
    assert(dedup || output.size() == total_size);
}

// auto find voters from other node group to make sure each ng members count is
// not less than replica_num
static inline bool AjustNgConfigs(
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs,
    uint32_t replica_num)
{
    auto ng_cnt = ng_configs.size();
    for (auto it = ng_configs.begin(); it != ng_configs.end(); it++)
    {
        if (it->second.size() >= replica_num)
        {
            continue;
        }

        uint32_t left_rep_cnt = replica_num - it->second.size();
        if (left_rep_cnt > (ng_cnt - 1))
        {
            left_rep_cnt = ng_cnt - 1;
        }

        for (size_t idx = 1; idx <= left_rep_cnt; idx++)
        {
            uint32_t nid = (it->first + idx) % ng_cnt;
            NodeConfig tmp_conf = ng_configs.at(nid).front();
            tmp_conf.is_candidate_ = false;
            it->second.emplace_back(std::move(tmp_conf));
        }
    }
    return true;
}

static inline bool ParseNgConfig(
    const std::string &ip_port_list,
    const std::string &standby_ip_port_list,
    const std::string &voter_ip_port_list,
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs,
    uint32_t replica_num,
    int16_t port_delta = 0)
{
    const char ng_delimiter = ',';
    const char node_delimiter = '|';
    // std::unordered_map<NodeGroupId, std::vector<NodeConfig>> ng_configs;
    std::unordered_map<std::string, NodeId> node_map;

    std::string token;
    std::istringstream tokenStream(ip_port_list);
    NodeGroupId ng_cnt = 0;
    while (std::getline(tokenStream, token, ng_delimiter))
    {
        size_t c_idx = token.find_first_of(':');
        if (c_idx == std::string::npos)
        {
            LOG(ERROR) << "port missing in ip_port_list: " << ip_port_list;
            return false;
        }

        auto it = node_map.find(token);
        if (it != node_map.end())
        {
            LOG(ERROR) << "Node repeated in config ip_port_list: " << token;
            return false;
        }

        uint16_t pt = std::stoi(token.substr(c_idx + 1)) + port_delta;
        std::string ip = token.substr(0, c_idx);
        NodeGroupId ng_id = ng_cnt++;
        NodeId node_id = ng_id;
        node_map.try_emplace(token, node_id);

        auto ins_res = ng_configs.try_emplace(ng_id);
        assert(ins_res.second);
        ins_res.first->second.emplace_back(NodeConfig(node_id, ip, pt, true));
    }

    // parse standby nodes.
    tokenStream.clear();
    tokenStream.str(standby_ip_port_list);
    std::istringstream tokenStream2;
    std::string token2;
    size_t s_ng_idx = 0;
    while (std::getline(tokenStream, token, ',') && s_ng_idx < ng_cnt)
    {
        tokenStream2.clear();
        tokenStream2.str(token);
        std::vector<NodeConfig> &members_vec = ng_configs.at(s_ng_idx);
        while (std::getline(tokenStream2, token2, node_delimiter))
        {
            size_t c_idx = token2.find_first_of(':');
            if (c_idx == std::string::npos)
            {
                LOG(ERROR) << "port missing in standby_ip_port_list: "
                           << token2;
                return false;
            }

            auto it = node_map.find(token2);
            if (it != node_map.end())
            {
                LOG(ERROR) << "Node in standby_ip_port_list also appear in "
                              "ip_port_list: "
                           << token2;
                return false;
            }

            uint16_t pt = std::stoi(token2.substr(c_idx + 1)) + port_delta;
            std::string ip = token2.substr(0, c_idx);
            NodeId node_id = node_map.size();
            node_map.try_emplace(token2, node_id);

            members_vec.emplace_back(NodeConfig(node_id, ip, pt, true));
        }
        s_ng_idx++;
    }

    // parse voters.
    tokenStream.clear();
    tokenStream.str(voter_ip_port_list);
    tokenStream2.clear();
    token2.clear();
    size_t v_ng_idx = 0;
    while (std::getline(tokenStream, token, ',') && v_ng_idx < ng_cnt)
    {
        tokenStream2.clear();
        tokenStream2.str(token);
        std::vector<NodeConfig> &members_vec = ng_configs.at(v_ng_idx);
        while (std::getline(tokenStream2, token2, node_delimiter))
        {
            size_t c_idx = token2.find_first_of(':');
            if (c_idx == std::string::npos)
            {
                LOG(ERROR) << "port missing in voter_ip_port_list: " << token2;
                return false;
            }

            uint16_t pt = std::stoi(token2.substr(c_idx + 1)) + port_delta;
            std::string ip = token2.substr(0, c_idx);

            auto it = node_map.find(token2);
            NodeId node_id;
            if (it != node_map.end())
            {
                node_id = it->second;
            }
            else
            {
                node_id = node_map.size();
                node_map.try_emplace(token2, node_id);
            }

            for (const NodeConfig &m_node : members_vec)
            {
                if (m_node.node_id_ == node_id)
                {
                    LOG(ERROR)
                        << "Voter node appeared in the same group: " << token2;
                    return false;
                }
            }

            members_vec.emplace_back(NodeConfig(node_id, ip, pt, false));
        }
        v_ng_idx++;
    }

    return AjustNgConfigs(ng_configs, replica_num);
}

static inline bool ReadClusterConfigFile(
    std::string &cluster_config_file_path,
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs,
    uint64_t &config_version)
{
    std::ifstream ifs(cluster_config_file_path);
    if (ifs.good())
    {
        std::string line;
        std::getline(ifs, line);
        size_t ng_cnt = std::stoul(line);
        for (size_t i = 0; i < ng_cnt; i++)
        {
            std::getline(ifs, line);
            std::istringstream iss(line);
            uint32_t ng_id;
            iss >> ng_id;

            std::vector<NodeConfig> nodes;
            uint32_t node_id;
            std::string host_name;
            uint16_t port;
            bool is_candidate;

            // Parse all nodes in this node group
            while (iss >> node_id >> host_name >> port >> is_candidate)
            {
                NodeConfig node_config(node_id, host_name, port, is_candidate);
                nodes.push_back(std::move(node_config));
            }

            ng_configs[ng_id] = std::move(nodes);
        }

        // Parse configuration version
        std::getline(ifs, line);
        config_version = std::stoull(line);
        LOG(INFO)
            << "Successfully loaded cluster config from file with version "
            << config_version;
        return true;
    }
    else
    {
        return false;
    }
}

static inline void ExtractNodesConfigs(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs,
    std::vector<NodeConfig> &nodes)
{
    std::unordered_set<NodeId> nid_set;
    for (const auto &[ng_id, ng_members] : ng_configs)
    {
        for (const auto &member : ng_members)
        {
            if (nid_set.find(member.node_id_) == nid_set.end())
            {
                nodes.emplace_back(NodeConfig(member));
                nid_set.emplace(member.node_id_);
            }
        }
    }
    std::sort(nodes.begin(),
              nodes.end(),
              [](auto &v1, auto &v2) { return v1.node_id_ < v2.node_id_; });
}

static inline void ExtractNodesConfigs(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs,
    std::unordered_map<NodeId, NodeConfig> &nodes)
{
    for (const auto &[ng_id, ng_members] : ng_configs)
    {
        for (const auto &member : ng_members)
        {
            if (nodes.find(member.node_id_) == nodes.end())
            {
                nodes.try_emplace(member.node_id_, NodeConfig(member));
            }
        }
    }
}

static inline int64_t PrimaryTermFromStandbyTerm(int64_t standby_term)
{
    return standby_term >> 32;
}

static inline uint32_t SubscribeIdFromStandbyTerm(int64_t standby_term)
{
    return standby_term & 0xFFFFFFFF;
}

static inline bool IsStandbyTx(int64_t tx_term)
{
    return (tx_term >> 32) > 0;
}

#if defined(WITH_JEMALLOC)
static inline void GetJemallocArenaStat(uint32_t arena_id,
                                        size_t &resident,
                                        size_t &allocated)
{
    // estimate thread memory usage from total process memory
    size_t total_resident = 0;
    size_t small_allocated = 0;
    size_t large_allocated = 0;
    size_t sz = sizeof(total_resident);

    // uint64_t epoch = 1;
    // mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch));

    char mib[64];
    snprintf(mib, sizeof(mib), "stats.arenas.%u.resident", arena_id);
    mallctl(mib, &total_resident, &sz, NULL, 0);
    snprintf(mib, sizeof(mib), "stats.arenas.%u.small.allocated", arena_id);
    mallctl(mib, &small_allocated, &sz, NULL, 0);
    snprintf(mib, sizeof(mib), "stats.arenas.%u.large.allocated", arena_id);
    mallctl(mib, &large_allocated, &sz, NULL, 0);

    resident = total_resident;
    allocated = small_allocated + large_allocated;
}
#endif
// TSC frequency in cycles per microsecond (measured at initialization)
inline std::once_flag tsc_frequency_initialized_;
inline std::atomic<uint64_t> tsc_cycles_per_microsecond_{0};

/** @brief
 * Measure TSC frequency by sleeping for 1ms and measuring cycles.
 * Retries until stable (within 1% difference) or up to 16ms total.
 * Should be called once during data substrate initialization.
 * This function is thread-safe and will only execute once.
 */
static void InitializeTscFrequency()
{
#if defined(__x86_64__) || defined(_M_X64)
    std::call_once(
        tsc_frequency_initialized_,
        []()
        {
            constexpr uint64_t SLEEP_MICROSECONDS = 1000;       // 1ms
            constexpr uint64_t MAX_TOTAL_MICROSECONDS = 16000;  // 16ms max
            constexpr double STABILITY_THRESHOLD =
                0.01;  // 1% difference for stability

            uint64_t prev_freq = 0;
            uint64_t total_slept = 0;
            int stable_count = 0;
            constexpr int REQUIRED_STABLE_COUNT =
                2;  // Need 2 consecutive stable measurements

            while (total_slept < MAX_TOTAL_MICROSECONDS)
            {
                uint64_t start_cycles = __rdtsc();
                std::this_thread::sleep_for(
                    std::chrono::microseconds(SLEEP_MICROSECONDS));
                uint64_t end_cycles = __rdtsc();
                uint64_t elapsed_cycles = end_cycles - start_cycles;
                uint64_t freq = elapsed_cycles /
                                SLEEP_MICROSECONDS;  // cycles per microsecond

                total_slept += SLEEP_MICROSECONDS;

                // Check if frequency is stable (within 1% of previous
                // measurement)
                if (prev_freq > 0)
                {
                    double diff_ratio =
                        (freq > prev_freq)
                            ? static_cast<double>(freq - prev_freq) / prev_freq
                            : static_cast<double>(prev_freq - freq) / prev_freq;
                    if (diff_ratio <= STABILITY_THRESHOLD)
                    {
                        stable_count++;
                        if (stable_count >= REQUIRED_STABLE_COUNT)
                        {
                            // Frequency is stable, use the average
                            tsc_cycles_per_microsecond_.store(
                                (prev_freq + freq) / 2,
                                std::memory_order_release);
                            return;
                        }
                    }
                    else
                    {
                        stable_count = 0;  // Reset stability counter
                    }
                }

                prev_freq = freq;
            }

            // If we couldn't get stable measurement, use the last measured
            // value
            if (prev_freq > 0)
            {
                tsc_cycles_per_microsecond_.store(prev_freq,
                                                  std::memory_order_release);
            }
            else
            {
                // Fallback to approximate value if measurement failed
                tsc_cycles_per_microsecond_.store(2000,
                                                  std::memory_order_release);
            }
        });  // End of lambda passed to std::call_once
#elif defined(__aarch64__)
    std::call_once(tsc_frequency_initialized_,
                   []()
                   {
                       uint64_t freq_hz;
                       __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(freq_hz));
                       tsc_cycles_per_microsecond_.store(
                           freq_hz / 1000000, std::memory_order_release);
                   });
#endif
}

/** @brief
 * Low-cost timing helper for approximate microsecond measurement.
 * Returns elapsed time in microseconds (precise for x86/ARM after
 * initialization, precise for fallback). Uses CPU-specific instructions for low
 * overhead on x86/ARM platforms.
 */
static inline uint64_t ReadTimeMicroseconds()
{
    return absl::GetCurrentTimeNanos() / 1000;
// #if defined(__x86_64__) || defined(_M_X64)
//     uint64_t cycles_per_us =
//         tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
//     if (__builtin_expect(cycles_per_us == 0, 0))
//     {
//         // Ensure TSC frequency is initialized (thread-safe, only initializes
//         // once)
//         InitializeTscFrequency();
//         cycles_per_us =
//             tsc_cycles_per_microsecond_.load(std::memory_order_acquire);
//     }
//     assert(cycles_per_us != 0);
//     // Read TSC (Time Stamp Counter) - returns CPU cycles
//     uint64_t cycles = __rdtsc();
//     return cycles / cycles_per_us;
// #elif defined(__aarch64__)
//     // Ensure ARM timer frequency is initialized (thread-safe, only initializes
//     // once)
//     uint64_t cycles_per_us =
//         tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
//     if (__builtin_expect(cycles_per_us == 0, 0))
//     {
//         InitializeTscFrequency();
//         cycles_per_us =
//             tsc_cycles_per_microsecond_.load(std::memory_order_acquire);
//     }
//     assert(cycles_per_us != 0);
//     // Read ARM virtual counter - returns timer ticks
//     uint64_t ticks;
//     __asm__ volatile("mrs %0, cntvct_el0" : "=r"(ticks));
//     return ticks / cycles_per_us;
// #else
//     // Fallback to std::chrono (slower but portable and precise)
//     using namespace std::chrono;
//     auto now = steady_clock::now();
//     return duration_cast<microseconds>(now.time_since_epoch()).count();
// #endif
}

}  // namespace txservice
