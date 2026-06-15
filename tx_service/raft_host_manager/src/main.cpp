#include <braft/raft.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sys/wait.h>

#include "INIReader.h"

#if BRPC_WITH_GLOG
#include <glog/logging.h>

#include <filesystem>
#include <iomanip>
#endif

#include <sys/prctl.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

#include "raft_host_manager.h"
#include "raft_host_manager_service.h"

using namespace host_manager;

DEFINE_string(hm_ip, "", "host manager ip address");
DEFINE_uint32(hm_port, 0, "host manager port");
DEFINE_string(hm_raft_log_path, "", "host manager raft log path");
DEFINE_string(cluster_config_path, "", "cluster config path");
DEFINE_bool(enable_brpc_builtin_services,
            true,
            "Enable to show brpc builtin services through http.");
DEFINE_string(service_bin_path, "", "runtime engine bin path");
DEFINE_string(data_substrate_config_path, "", "data substrate config path");
DEFINE_string(engine_specific_config_path, "", "engine specific config path");
DEFINE_string(config, "", "Configuration");
DEFINE_bool(fork_from_txservice, false, "fork host manager from txservice");

static bool CheckCommandLineFlagIsDefault(const char *name)
{
    gflags::CommandLineFlagInfo flag_info;

    bool flag_found = gflags::GetCommandLineFlagInfo(name, &flag_info);
    // Make sure the flag is declared.
    assert(flag_found);
    (void) flag_found;

    // Return `true` if the flag has the default value and has not been set
    // explicitly from the cmdline or via SetCommandLineOption
    return flag_info.is_default;
}

void hm_exit_handler(int sig_num)
{
    TxNodeStatus expected = TxNodeStatus::Started;

    while (!RaftHostManager::Instance().tx_node_status_.compare_exchange_strong(
        expected,
        TxNodeStatus::Terminated,
        std::memory_order_acquire,
        std::memory_order_relaxed))
    {
        if (expected == TxNodeStatus::Terminated)
        {
            // No op if someone else is handling termination.
            return;
        }
    }

    if (sig_num == SIGCHLD)
    {
        // Fail to start tx service. don't exit host manager process.
        LOG(ERROR) << "Receive SIGCHLD signal, failed to init txservice";
    }

    if (expected == TxNodeStatus::Started || sig_num == SIGCHLD)
    {
        // Shutdown servers if it's started.
        LOG(INFO)
            << "Shutting down host manager process because tx service is dead";
        RaftHostManager::Instance().Shutdown();
        LOG(INFO) << "Host Manager shutdown complete";
    }

    exit(0);
}

void init_signal()
{
    sigset_t set;
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sa.sa_handler = hm_exit_handler;
    sigaction(SIGHUP, &sa, (struct sigaction *) 0);
    sigaction(SIGINT, &sa, (struct sigaction *) 0);
    sigaction(SIGCHLD, &sa, (struct sigaction *) 0);
    sigaddset(&set, SIGHUP);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGKILL);
    sigaddset(&set, SIGCHLD);
    sigprocmask(SIG_UNBLOCK, &set, NULL);
}

#if BRPC_WITH_GLOG
inline void CustomPrefix(std::ostream &s,
                         const google::LogMessageInfo &l,
                         void *)
{
    s << "["                                   //
      << std::setw(4) << 1900 + l.time.year()  // YY
      << '-'                                   // -
      << std::setw(2) << 1 + l.time.month()    // MM
      << '-'                                   // -
      << std::setw(2) << l.time.day()          // DD
      << 'T'                                   // T
      << std::setw(2) << l.time.hour()         // hh
      << ':'                                   // :
      << std::setw(2) << l.time.min()          // mm
      << ':'                                   // :
      << std::setw(2) << l.time.sec()          // ss
      << '.'                                   // .
      << std::setfill('0') << std::setw(6)     //
      << l.time.usec()                         // usec
      << " " << l.severity[0] << " "
      << "" << l.thread_id << "] "
#ifndef DISABLE_CODE_LINE_IN_LOG
      << "[" << l.filename << ':' << l.line_number << "]";
#else
        ;
#endif
};

inline void InitGoogleLogging(char **argv)
{
    // FLAGS_logtostderr = true;

    // If `GLOG_logtostderr` is specified then log to `stderr` only .
    //
    // NOTE: This is for cases where disk space needs protection, like when
    // deployed in the cloud.
    if (FLAGS_logtostderr && FLAGS_log_dir.empty())
    {
        FLAGS_alsologtostderr = false;
        FLAGS_logtostdout = false;
    }
    else
    {
        if (FLAGS_log_dir.empty())
        {
            // Get the absolute path of the bin directory
            char bin_path[PATH_MAX];
            ssize_t len = readlink("/proc/self/exe", bin_path, PATH_MAX);
            std::filesystem::path fullPath(std::string(bin_path, len));
            std::filesystem::path dir_path =
                fullPath.parent_path().parent_path();
            FLAGS_log_dir = dir_path.string() + "/logs";
        }

        if (!std::filesystem::exists(FLAGS_log_dir))
        {
            std::filesystem::create_directories(FLAGS_log_dir);
        }

        // Log to stderr and logfiles
        // FLAGS_alsologtostderr = true;

        // NOTE: Enable this will log to `stdout` instead of logfiles.
        FLAGS_logtostdout = false;

        // NOTE: Enable this will log to `stderr` instead of logfiles.
        FLAGS_logtostderr = false;

        // Log INFO/WARNING/ERROR/FATAL
        FLAGS_minloglevel = 0;

        // stderrthreshold (log messages at or above this level are copied to
        // stderr in addition to logfiles.) default: 2.
        FLAGS_stderrthreshold = google::GLOG_FATAL;

        // Don't buffer anything. NOTE: If `logtostderr` or `logtostdout` is
        // `true` then glog will force this value to -1.
        FLAGS_logbuflevel = -1;

        FLAGS_log_file_header = false;

        auto log_file_name_prefix = "host_manager.log";
        auto sep = std::filesystem::path::preferred_separator;
        auto log_file_prefix = FLAGS_log_dir + sep + log_file_name_prefix + ".";

        // Configure log destinations.
        google::SetLogDestination(google::INFO,
                                  (log_file_prefix + "INFO.").c_str());
        google::SetLogDestination(google::WARNING,
                                  (log_file_prefix + "WARNING.").c_str());
        google::SetLogDestination(google::ERROR,
                                  (log_file_prefix + "ERROR.").c_str());

        // Configure symlink for logfiles.
        google::SetLogSymlink(google::INFO, log_file_name_prefix);
        google::SetLogSymlink(google::WARNING, log_file_name_prefix);
        google::SetLogSymlink(google::ERROR, log_file_name_prefix);
    }
    google::InitGoogleLogging(argv[0], &CustomPrefix);
}

#endif

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);

#if BRPC_WITH_GLOG
    InitGoogleLogging(argv);
#endif

    std::string config_file = FLAGS_config;
    INIReader config_reader(config_file);

    if (!config_file.empty() && config_reader.ParseError())
    {
        LOG(ERROR) << "Error: Can't load config file.";
        return -1;
    }

    if (CheckCommandLineFlagIsDefault("hm_ip"))
    {
        if (!config_reader.HasValue("local", "hm_ip"))
        {
            LOG(ERROR) << "Can not read the value of `hm_ip` from config file "
                          "or command flags";
            return -1;
        }
        else
        {
            FLAGS_hm_ip =
                config_reader.GetString("local", "hm_ip", FLAGS_hm_ip);
        }
    }

    if (CheckCommandLineFlagIsDefault("hm_port"))
    {
        if (!config_reader.HasValue("local", "hm_port"))
        {
            LOG(ERROR) << "Can not read the value of `hm_port` from config "
                          "file or command flags";
            return -1;
        }
        else
        {
            FLAGS_hm_port =
                config_reader.GetInteger("local", "hm_port", FLAGS_hm_port);
        }
    }

    if (CheckCommandLineFlagIsDefault("hm_raft_log_path"))
    {
        if (!config_reader.HasValue("local", "hm_raft_log_path"))
        {
            LOG(ERROR) << "Can not read the value of `hm_raft_log_path` from "
                          "config file or command flags";
            return -1;
        }
        else
        {
            FLAGS_hm_raft_log_path = config_reader.GetString(
                "local", "hm_raft_log_path", FLAGS_hm_raft_log_path);
        }
    }

    if (CheckCommandLineFlagIsDefault("cluster_config_path"))
    {
        FLAGS_cluster_config_path = config_reader.GetString(
            "local", "cluster_config_path", FLAGS_cluster_config_path);
    }

    FLAGS_enable_brpc_builtin_services =
        !CheckCommandLineFlagIsDefault("enable_brpc_builtin_services")
            ? FLAGS_enable_brpc_builtin_services
            : config_reader.GetBoolean("local",
                                       "enable_brpc_builtin_services",
                                       FLAGS_enable_brpc_builtin_services);

    FLAGS_service_bin_path =
        !CheckCommandLineFlagIsDefault("service_bin_path")
            ? FLAGS_service_bin_path
            : config_reader.GetString(
                  "local", "service_bin_path", FLAGS_service_bin_path);

    FLAGS_data_substrate_config_path =
        !CheckCommandLineFlagIsDefault("data_substrate_config_path")
            ? FLAGS_data_substrate_config_path
            : config_reader.GetString("local",
                                      "data_substrate_config_path",
                                      FLAGS_data_substrate_config_path);

    FLAGS_engine_specific_config_path =
        !CheckCommandLineFlagIsDefault("engine_specific_config_path")
            ? FLAGS_engine_specific_config_path
            : config_reader.GetString("local",
                                      "engine_specific_config_path",
                                      FLAGS_engine_specific_config_path);

    // Reset signal mask first. Parent process might have some signals
    // blocked.
    init_signal();
    // Send SIGHUP to this process to exit if parent tx process dies.
    int r = prctl(PR_SET_PDEATHSIG, SIGHUP);
    if (r == -1)
    {
        LOG(ERROR) << "Parent tx service process already died";
        return -1;
    }

    //
    LOG(INFO) << "ip: " << FLAGS_hm_ip << ", port: " << FLAGS_hm_port
              << ", raft log path: " << FLAGS_hm_raft_log_path
              << ", tx service bin path: " << FLAGS_service_bin_path
              << ", tx service config path: "
              << FLAGS_data_substrate_config_path
              << ", yaml config path: " << FLAGS_engine_specific_config_path;

    // Set check health interval and circuit breaker threshold so that
    // new started node can be connected to the cluster before it triggers
    // reelection.
    GFLAGS_NAMESPACE::SetCommandLineOption("health_check_interval", "1");
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "500");

    // Start host manager server first
    if (!RaftHostManager::Instance().Start(FLAGS_hm_ip,
                                           FLAGS_hm_port,
                                           FLAGS_hm_raft_log_path,
                                           FLAGS_enable_brpc_builtin_services,
                                           FLAGS_service_bin_path,
                                           FLAGS_data_substrate_config_path,
                                           FLAGS_cluster_config_path,
                                           FLAGS_engine_specific_config_path,
                                           FLAGS_fork_from_txservice))
    {
        return -1;
    }

    // Sleep forever until tx processor is dead. The registered exit handler
    // will exit this process after tx service process is dead.
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(100));
    }

    return 0;
}
