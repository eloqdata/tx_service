#include <INIReader.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <limits.h>

#include "data_substrate.h"
#include "eloq_log_wrapper.h"
#include "sequences/sequences.h"
#include "tx_service.h"
DEFINE_int32(checkpointer_interval, 10, "Checkpointer interval in seconds");
DEFINE_int32(checkpointer_min_ckpt_request_interval,
             5,
             "Minimum checkpoint request interval in seconds, to avoid too "
             "frequent checkpoint requests");
DEFINE_int32(range_slice_memory_limit_percent,
             10,
             "Range slice memory limit percentage");
DEFINE_int32(checkpointer_delay_seconds, 5, "Checkpointer delay in seconds");
DEFINE_int32(collect_active_tx_ts_interval_seconds,
             2,
             "Active transaction timestamp collection interval");
DEFINE_bool(kickout_data_for_test, false, "Kickout data for test");
DEFINE_bool(enable_key_cache, false, "Enable key cache");
DEFINE_uint32(max_standby_lag,
              400000,
              "txservice max msg lag between primary and standby");
DEFINE_string(tx_service_data_path, "", "path for tx_service data");
DEFINE_bool(fork_host_manager, true, "fork host manager process");
DEFINE_string(hm_ip, "", "Host manager IP");
DEFINE_int32(hm_port, 0, "Host manager port");
DEFINE_string(hm_bin_path,
              "",
              "host manager binary path if forking host manager process from "
              "main process");
DEFINE_uint32(deadlock_check_interval_seconds,
              10,
              "Deadlock check interval in seconds");
DEFINE_bool(realtime_sampling,
            true,
            "Whether enable realtime sampling. If disable it, user may need "
            "to execute "
            "analyze command at some time. Different from Innodb, Eloq never "
            "analyze table automatically.");
DEFINE_uint32(range_split_worker_num, 0, "Range split worker number");
DEFINE_bool(auto_redirect,
            false,
            "If redirect remote object command to remote node internally");

// Checkpoint trigger threshold based on dirty memory size
// Default is 0, which means 10% of memory_limit will be used as threshold
DEFINE_uint64(dirty_memory_check_interval,
              1000,
              "Check dirty memory every N calls to AdjustDataKeyStats");
DEFINE_uint64(dirty_memory_size_threshold_mb,
              0,
              "Trigger checkpoint when dirty memory exceeds this (MB). "
              "0 means use 10% of memory_limit as default");

bool DataSubstrate::InitializeTxService(const INIReader &config_reader)
{
    uint64_t checkpointer_interval =
        !CheckCommandLineFlagIsDefault("checkpointer_interval")
            ? FLAGS_checkpointer_interval
            : config_reader.GetInteger("local",
                                       "checkpointer_interval",
                                       FLAGS_checkpointer_interval);

    uint64_t checkpointer_delay_seconds =
        !CheckCommandLineFlagIsDefault("checkpointer_delay_seconds")
            ? FLAGS_checkpointer_delay_seconds
            : config_reader.GetInteger("local",
                                       "checkpointer_delay_seconds",
                                       FLAGS_checkpointer_delay_seconds);

    uint64_t checkpointer_min_ckpt_request_interval =
        !CheckCommandLineFlagIsDefault("checkpointer_min_ckpt_request_interval")
            ? FLAGS_checkpointer_min_ckpt_request_interval
            : config_reader.GetInteger(
                  "local",
                  "checkpointer_min_ckpt_request_interval",
                  FLAGS_checkpointer_min_ckpt_request_interval);

    uint64_t collect_active_tx_ts_interval_seconds =
        !CheckCommandLineFlagIsDefault("collect_active_tx_ts_interval_seconds")
            ? FLAGS_collect_active_tx_ts_interval_seconds
            : config_reader.GetInteger(
                  "local",
                  "collect_active_tx_ts_interval_seconds",
                  FLAGS_collect_active_tx_ts_interval_seconds);

    uint64_t max_standby_lag =
        !CheckCommandLineFlagIsDefault("max_standby_lag")
            ? FLAGS_max_standby_lag
            : config_reader.GetInteger(
                  "local", "max_standby_lag", FLAGS_max_standby_lag);

    bool kickout_data_for_test =
        !CheckCommandLineFlagIsDefault("kickout_data_for_test")
            ? FLAGS_kickout_data_for_test
            : config_reader.GetBoolean("local",
                                       "kickout_data_for_test",
                                       FLAGS_kickout_data_for_test);

    bool enable_key_cache =
        !CheckCommandLineFlagIsDefault("enable_key_cache")
            ? FLAGS_enable_key_cache
            : config_reader.GetBoolean(
                  "local", "enable_key_cache", FLAGS_enable_key_cache);

    std::string tx_service_data_path =
        !CheckCommandLineFlagIsDefault("tx_service_data_path")
            ? FLAGS_tx_service_data_path
            : config_reader.GetString(
                  "local", "tx_service_data_path", FLAGS_tx_service_data_path);
    std::string tx_path("local://");
    if (tx_service_data_path.empty())
    {
        tx_path.append(core_config_.data_path);
    }
    else
    {
        tx_path.append(tx_service_data_path);
    }

    uint64_t range_slice_memory_limit_percent =
        !CheckCommandLineFlagIsDefault("range_slice_memory_limit_percent")
            ? FLAGS_range_slice_memory_limit_percent
            : config_reader.GetInteger("local",
                                       "range_slice_memory_limit_percent",
                                       FLAGS_range_slice_memory_limit_percent);

    uint64_t deadlock_check_interval_seconds =
        !CheckCommandLineFlagIsDefault("deadlock_check_interval_seconds")
            ? FLAGS_deadlock_check_interval_seconds
            : config_reader.GetInteger("local",
                                       "deadlock_check_interval_seconds",
                                       FLAGS_deadlock_check_interval_seconds);
    txservice::DeadLockCheck::SetTimeInterval(deadlock_check_interval_seconds);

    bool realtime_sampling =
        !CheckCommandLineFlagIsDefault("realtime_sampling")
            ? FLAGS_realtime_sampling
            : config_reader.GetBoolean(
                  "local", "realtime_sampling", FLAGS_realtime_sampling);

    uint64_t range_split_worker_num =
        !CheckCommandLineFlagIsDefault("range_split_worker_num")
            ? FLAGS_range_split_worker_num
            : config_reader.GetInteger("local",
                                       "range_split_worker_num",
                                       FLAGS_range_split_worker_num);

    bool auto_redirect =
        !CheckCommandLineFlagIsDefault("auto_redirect")
            ? FLAGS_auto_redirect
            : config_reader.GetBoolean(
                  "local", "auto_redirect", FLAGS_auto_redirect);

    uint64_t dirty_memory_check_interval =
        !CheckCommandLineFlagIsDefault("dirty_memory_check_interval")
            ? FLAGS_dirty_memory_check_interval
            : config_reader.GetInteger("local",
                                       "dirty_memory_check_interval",
                                       FLAGS_dirty_memory_check_interval);

    uint64_t dirty_memory_size_threshold_mb =
        !CheckCommandLineFlagIsDefault("dirty_memory_size_threshold_mb")
            ? FLAGS_dirty_memory_size_threshold_mb
            : config_reader.GetInteger("local",
                                       "dirty_memory_size_threshold_mb",
                                       FLAGS_dirty_memory_size_threshold_mb);

    bool fork_hm_process = false;
    std::string hm_ip = "";
    std::string hm_bin_path = "";
    uint16_t hm_port = 0;
    if (!core_config_.bootstrap)
    {
        fork_hm_process =
            !CheckCommandLineFlagIsDefault("fork_host_manager")
                ? FLAGS_fork_host_manager
                : config_reader.GetBoolean(
                      "local", "fork_host_manager", FLAGS_fork_host_manager);
        hm_ip = !CheckCommandLineFlagIsDefault("hm_ip")
                    ? FLAGS_hm_ip
                    : config_reader.Get("local", "hm_ip", FLAGS_hm_ip);

        hm_port =
            !CheckCommandLineFlagIsDefault("hm_port")
                ? FLAGS_hm_port
                : config_reader.GetInteger("local", "hm_port", FLAGS_hm_port);

        hm_bin_path =
            !CheckCommandLineFlagIsDefault("hm_bin_path")
                ? FLAGS_hm_bin_path
                : config_reader.Get("local", "hm_bin_path", FLAGS_hm_bin_path);
#ifdef FORK_HM_PROCESS
        if (hm_ip.empty())
        {
            hm_ip = network_config_.local_ip;
        }
        if (hm_port == 0)
        {
            hm_port = network_config_.local_port + 4;
        }
        if (hm_bin_path.empty())
        {
            char path_buf[PATH_MAX];
            ssize_t len =
                ::readlink("/proc/self/exe", path_buf, sizeof(path_buf) - 1);
            if (len < 0 || len >= (ssize_t) sizeof(path_buf))
            {
                LOG(ERROR) << "readlink(/proc/self/exe) failed; cannot derive "
                              "host_manager path";
                return false;
            }
            path_buf[len] = '\0';
            std::string s_path(path_buf);
            std::string::size_type pos = s_path.find_last_of("/");
            std::string parent_path = s_path.substr(0, pos);
            hm_bin_path = parent_path + "/host_manager";
        }
#endif
    }

    LOG(INFO) << "Data substrate memory limit: "
              << core_config_.node_memory_limit_mb << "MB";

    std::map<std::string, uint32_t> tx_service_conf{
        {"core_num", core_config_.core_num},
        {"checkpointer_interval", checkpointer_interval},
        {"checkpointer_min_ckpt_request_interval",
         checkpointer_min_ckpt_request_interval},
        {"node_memory_limit_mb", core_config_.node_memory_limit_mb},
        {"checkpointer_delay_seconds", checkpointer_delay_seconds},
        {"collect_active_tx_ts_interval_seconds",
         collect_active_tx_ts_interval_seconds},
        {"realtime_sampling", realtime_sampling ? 1 : 0},
        {"rep_group_cnt", network_config_.node_group_replica_num},
        {"range_split_worker_num", range_split_worker_num},
        {"enable_shard_heap_defragment",
         core_config_.enable_heap_defragment ? 1 : 0},
        {"enable_key_cache", enable_key_cache},
        {"max_standby_lag", max_standby_lag},
        {"kickout_data_for_test", kickout_data_for_test ? 1 : 0},
        {"range_slice_memory_limit_percent", range_slice_memory_limit_percent},
        {"dirty_memory_check_interval",
         static_cast<uint32_t>(std::min(dirty_memory_check_interval,
                                        static_cast<uint64_t>(UINT32_MAX)))},
        {"dirty_memory_size_threshold_mb",
         static_cast<uint32_t>(std::min(dirty_memory_size_threshold_mb,
                                        static_cast<uint64_t>(UINT32_MAX)))}};

    txservice::CatalogFactory *catalog_factory[NUM_EXTERNAL_ENGINES] = {
        nullptr, nullptr, nullptr};

    for (size_t i = 0; i < NUM_EXTERNAL_ENGINES; i++)
    {
        catalog_factory[i] = engines_[i].catalog_factory;
    }

    auto log_agent = std::make_unique<txservice::EloqLogAgent>(
        log_service_config_.txlog_group_replica_num);

    tx_service_ = std::make_unique<txservice::TxService>(
        catalog_factory,
        system_handler_,
        tx_service_conf,
        network_config_.node_id,
        network_config_.native_ng_id,
        &network_config_.ng_configs,
        network_config_.cluster_config_version,
        core_config_.enable_data_store ? store_hd_.get() : nullptr,
        log_agent.get(),
        core_config_.enable_mvcc,               // enable_mvcc
        !core_config_.enable_wal,               // skip_wal
        !core_config_.enable_data_store,        // skip_kv
        core_config_.enable_cache_replacement,  // enable_cache_replacement
        auto_redirect,                          // auto_redirect
        metrics_registry_.get(),                // metrics_registry
        tx_service_common_labels_,              // common_labels
        &prebuilt_tables_,
        publish_func_,
        external_metrics_);

    if (core_config_.enable_data_store)
    {
        store_hd_->SetTxService(tx_service_.get());
    }

    if (tx_service_->Start(network_config_.node_id,
                           network_config_.native_ng_id,
                           &network_config_.ng_configs,
                           network_config_.cluster_config_version,
                           &log_service_config_.txlog_ips,
                           &log_service_config_.txlog_ports,
                           &hm_ip,
                           &hm_port,
                           &hm_bin_path,
                           tx_service_conf,
                           std::move(log_agent),
                           tx_path,
                           network_config_.cluster_config_file_path,
                           fork_hm_process) != 0)
    {
        LOG(ERROR) << "Failed to start tx service!!!!!";
        return false;
    }

    txservice::Sequences::InitSequence(tx_service_.get(), store_hd_.get());

    // tx_service is a distributed service, should wait for all the tx_service
    // nodes to finish the log recovery process and setup the cc_stream_sender.
    tx_service_->WaitClusterReady();
    // wait for the tx_service node to become the native group leader.
    // tx_service_->WaitNodeBecomeNativeGroupLeader();

    return true;
}
