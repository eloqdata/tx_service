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

#include <cstdint>
#include <map>
#include <optional>
#include <unordered_set>

#include "sharder.h"
#include "tx_record.h"
#include "tx_serialize.h"
#include "type.h"
namespace txservice
{

/**
 * @brief The cluster gate: a cluster-wide flag that, while paused, makes every
 * new schema op fail with TxErrorCode::CLUSTER_GATED (EloqDoc
 * docs/cdc-change-stream.md §6.2).
 *
 * CDC bootstrap pauses it so that no DDL runs while it copies the catalog. The
 * cluster config cc map keeps the latest value in its single entry's payload,
 * where each schema op's cluster config read lock reads it.
 */
struct ClusterGate
{
    /** @brief What a pause rejects. */
    enum class Mode : uint8_t
    {
        kDdl = 0,     ///< New create, drop, truncate and index ops.
        kDdlDml = 1,  ///< Also the first write of a transaction; reserved.
    };

    Mode mode{Mode::kDdl};  ///< What the gate rejects while paused.
    bool paused{false};     ///< Whether the gate is closed.

    /**
     * @brief Compares two gates.
     *
     * @param rhs The other gate.
     * @return true if both have the same mode and state.
     */
    bool operator==(const ClusterGate &rhs) const
    {
        return mode == rhs.mode && paused == rhs.paused;
    }
};

struct ClusterConfigRecord : public TxRecord
{
public:
    ClusterConfigRecord() = default;
    ClusterConfigRecord(const ClusterConfigRecord &rhs)
        : is_config_owner_(rhs.is_config_owner_),
          version_(rhs.version_),
          cluster_gate_(rhs.cluster_gate_)
    {
        if (rhs.is_config_owner_)
        {
            // A deserialized record owns its configs, and may have none: a
            // cluster gate's carries none.
            if (rhs.node_group_configs_uptr_ == nullptr)
            {
                return;
            }
            node_group_configs_uptr_ = std::make_unique<
                std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
            for (const auto &pair : *rhs.node_group_configs_uptr_)
            {
                node_group_configs_uptr_->emplace(pair.first, pair.second);
            }
        }
        else
        {
            node_group_configs_ptr_ = rhs.node_group_configs_ptr_;
        }
    }

    ClusterConfigRecord &operator=(const ClusterConfigRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        version_ = rhs.version_;
        cluster_gate_ = rhs.cluster_gate_;
        if (is_config_owner_ && node_group_configs_uptr_ != nullptr)
        {
            node_group_configs_uptr_.reset();
        }
        is_config_owner_ = rhs.is_config_owner_;
        if (rhs.is_config_owner_)
        {
            if (rhs.node_group_configs_uptr_ != nullptr)
            {
                node_group_configs_uptr_ = std::make_unique<
                    std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
                for (const auto &pair : *rhs.node_group_configs_uptr_)
                {
                    node_group_configs_uptr_->emplace(pair.first, pair.second);
                }
            }
        }
        else
        {
            node_group_configs_ptr_ = rhs.node_group_configs_ptr_;
        }

        return *this;
    }
    ~ClusterConfigRecord()
    {
        if (is_config_owner_ && node_group_configs_uptr_ != nullptr)
        {
            node_group_configs_uptr_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *node_group_configs = nullptr;
        if (is_config_owner_)
        {
            node_group_configs = node_group_configs_uptr_.get();
        }
        else
        {
            node_group_configs = node_group_configs_ptr_;
        }
        bool has_ng_config = node_group_configs != nullptr;
        SerializeToStr(&has_ng_config, str);
        if (node_group_configs)
        {
            uint16_t ng_count =
                static_cast<uint16_t>(node_group_configs->size());
            SerializeToStr(&ng_count, str);
            for (auto &pair : *node_group_configs)
            {
                SerializeToStr(&pair.first, str);
                uint16_t node_count = static_cast<uint16_t>(pair.second.size());
                SerializeToStr(&node_count, str);
                for (auto &node_config : pair.second)
                {
                    node_config.Serialize(str);
                }
            }
        }
        SerializeToStr(&version_, str);
        // The gate goes last, behind its own has-bit, as the configs go behind
        // has_ng_config.
        bool has_gate = cluster_gate_.has_value();
        SerializeToStr(&has_gate, str);
        if (has_gate)
        {
            uint8_t mode = static_cast<uint8_t>(cluster_gate_->mode);
            SerializeToStr(&mode, str);
            SerializeToStr(&cluster_gate_->paused, str);
        }
    }

    size_t SerializedLength() const override
    {
        size_t len = 0;
        len += sizeof(bool);
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *node_group_configs = nullptr;
        if (is_config_owner_)
        {
            node_group_configs = node_group_configs_uptr_.get();
        }
        else
        {
            node_group_configs = node_group_configs_ptr_;
        }
        if (node_group_configs)
        {
            len += sizeof(uint16_t);
            for (auto &pair : *node_group_configs)
            {
                len += (sizeof(uint32_t) + sizeof(uint16_t));
                for (auto &node_config : pair.second)
                {
                    len += node_config.SerializedLength();
                }
            }
        }

        len += sizeof(uint64_t);
        len += sizeof(bool);
        if (cluster_gate_.has_value())
        {
            len += sizeof(uint8_t) + sizeof(bool);
        }
        return len;
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
        bool has_ng_config;
        DesrializeFrom(buf, offset, &has_ng_config);
        // Deserialize always makes this record the owner.
        is_config_owner_ = true;
        if (is_config_owner_ && node_group_configs_uptr_ != nullptr)
        {
            node_group_configs_uptr_.reset();
            node_group_configs_ptr_ = nullptr;
        }
        if (has_ng_config)
        {
            node_group_configs_uptr_ = std::make_unique<
                std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
            uint16_t ng_count;
            DesrializeFrom(buf, offset, &ng_count);
            for (uint16_t i = 0; i < ng_count; i++)
            {
                NodeGroupId ng_id;
                DesrializeFrom(buf, offset, &ng_id);
                uint16_t node_count;
                DesrializeFrom(buf, offset, &node_count);
                std::vector<NodeConfig> node_configs;
                node_configs.reserve(node_count);
                for (uint16_t j = 0; j < node_count; j++)
                {
                    NodeConfig node_config;
                    node_config.Deserialize(buf, offset);
                    node_configs.push_back(std::move(node_config));
                }
                node_group_configs_uptr_->emplace(ng_id,
                                                  std::move(node_configs));
            }
        }
        DesrializeFrom(buf, offset, &version_);
        bool has_gate;
        DesrializeFrom(buf, offset, &has_gate);
        cluster_gate_.reset();
        if (has_gate)
        {
            uint8_t mode;
            DesrializeFrom(buf, offset, &mode);
            ClusterGate gate;
            gate.mode = static_cast<ClusterGate::Mode>(mode);
            DesrializeFrom(buf, offset, &gate.paused);
            cluster_gate_ = gate;
        }
    }

    TxRecord::Uptr Clone() const override
    {
        auto uptr = std::make_unique<ClusterConfigRecord>();
        uptr->version_ = version_;
        uptr->cluster_gate_ = cluster_gate_;
        uptr->is_config_owner_ = true;
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *node_group_configs = nullptr;
        if (is_config_owner_)
        {
            node_group_configs = node_group_configs_uptr_.get();
        }
        else
        {
            node_group_configs = node_group_configs_ptr_;
        }

        if (node_group_configs)
        {
            uptr->node_group_configs_uptr_ = std::make_unique<
                std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
            for (auto &pair : *node_group_configs)
            {
                uptr->node_group_configs_uptr_->emplace(pair.first,
                                                        pair.second);
            }
        }
        return uptr;
    }

    void Copy(const TxRecord &record) override
    {
        if (this == &record)
        {
            return;
        }
        if (is_config_owner_ && node_group_configs_uptr_)
        {
            node_group_configs_uptr_.reset();
        }
        const ClusterConfigRecord &rhs =
            dynamic_cast<const ClusterConfigRecord &>(record);
        version_ = rhs.version_;
        cluster_gate_ = rhs.cluster_gate_;
        is_config_owner_ = rhs.is_config_owner_;
        if (rhs.is_config_owner_)
        {
            if (rhs.node_group_configs_uptr_ != nullptr)
            {
                node_group_configs_uptr_ = std::make_unique<
                    std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
                for (auto &pair : *rhs.node_group_configs_uptr_)
                {
                    node_group_configs_uptr_->emplace(pair.first, pair.second);
                }
            }
        }
        else
        {
            node_group_configs_ptr_ = rhs.node_group_configs_ptr_;
        }
    }

    std::string ToString() const override
    {
        return std::string("");
    }

    size_t Size() const override
    {
        return sizeof(*this);
    }

    size_t MemUsage() const override
    {
        if (node_group_configs_uptr_)
        {
            return sizeof(*this) + sizeof(*node_group_configs_uptr_);
        }
        return sizeof(*this);
    }

    void SetVersion(uint64_t version)
    {
        version_ = version;
    }

    uint64_t Version() const
    {
        return version_;
    }

    void SetNodeGroupConfigs(
        std::unique_ptr<
            std::unordered_map<NodeGroupId, std::vector<NodeConfig>>> configs)
    {
        node_group_configs_uptr_ = std::move(configs);
        is_config_owner_ = true;
    }

    void SetNodeGroupConfigs(
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> *configs)
    {
        if (is_config_owner_ && node_group_configs_uptr_ != nullptr)
        {
            node_group_configs_uptr_.reset();
        }
        node_group_configs_ptr_ = configs;
        is_config_owner_ = false;
    }

    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &
    GetNodeGroupConfigs() const
    {
        return is_config_owner_ ? *node_group_configs_uptr_
                                : *node_group_configs_ptr_;
    }

    /**
     * @brief The cluster gate the record carries.
     *
     * Absent and present with paused = false are different values: the
     * first says nothing, the second is a resume. A record carrying a gate
     * carries no node group configs.
     *
     * @return The gate, or none.
     */
    const std::optional<ClusterGate> &GetClusterGate() const
    {
        return cluster_gate_;
    }

    /**
     * @brief Sets the cluster gate the record carries.
     *
     * @param gate The gate.
     */
    void SetClusterGate(const ClusterGate &gate)
    {
        cluster_gate_ = gate;
    }

    /**
     * @brief Whether the record carries a closed gate in mode kDdl or
     * kDdlDml, which rejects new schema ops.
     *
     * @return true if a new schema op must fail with CLUSTER_GATED.
     */
    bool GatesSchemaOps() const
    {
        return cluster_gate_.has_value() && cluster_gate_->paused;
    }

    void Reset()
    {
        if (is_config_owner_ && node_group_configs_uptr_)
        {
            node_group_configs_uptr_.reset();
        }
        is_config_owner_ = false;
        node_group_configs_ptr_ = nullptr;
        version_ = 0;
        cluster_gate_.reset();
    }

private:
    // Only used during post write all to broadcast new config. When directing
    // to local node, we use raw pointer to reference struct owned by cluster
    // scale operation. When directing to remote node, the record will be owner
    // of the struct.
    union
    {
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *node_group_configs_ptr_;
        std::unique_ptr<
            std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>
            node_group_configs_uptr_{nullptr};
    };
    bool is_config_owner_{false};
    uint64_t version_{0};
    /**
     * The cluster gate: in the cluster config cc map's entry, the latest value
     * a gate op installed; in a gate op's post write all, the value to
     * install. Absent everywhere else.
     */
    std::optional<ClusterGate> cluster_gate_;
};
}  // namespace txservice
