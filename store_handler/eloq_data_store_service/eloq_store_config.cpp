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
#include "eloq_store_config.h"

#include <cassert>
#include <filesystem>
#include <regex>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include <sstream>

DEFINE_uint32(eloq_store_worker_num, 1, "EloqStore server worker num.");

DEFINE_string(eloq_store_data_path_list,
              "",
              "The data paths of the EloqStore (default is "
              "'{eloq_data_path}/eloq_dss/eloqstore_data').");
DEFINE_uint32(eloq_store_open_files_limit,
              1024,
              "EloqStore maximum open files.");
DEFINE_string(eloq_store_cloud_store_path,
              "",
              "EloqStore cloud store path (disable cloud store if empty)");
DEFINE_uint32(eloq_store_cloud_worker_count,
              1,
              "EloqStore cloud worker count.");
DEFINE_uint32(eloq_store_data_page_restart_interval,
              16,
              "EloqStore data page restart interval.");
DEFINE_uint32(eloq_store_index_page_restart_interval,
              16,
              "EloqStore index page restart interval.");
DEFINE_uint32(eloq_store_init_page_count,
              1 << 15,
              "EloqStore initial page count.");
DEFINE_bool(eloq_store_skip_verify_checksum,
            false,
            "EloqStore skip verify checksum.");
DEFINE_string(eloq_store_index_buffer_pool_size,
              "128MB",
              "EloqStore index buffer pool size.");
DEFINE_uint32(eloq_store_manifest_limit, 8 << 20, "EloqStore manifest limit.");
DEFINE_uint32(eloq_store_io_queue_size,
              4096,
              "EloqStore io queue size per shard.");
DEFINE_uint32(eloq_store_max_inflight_write,
              64 << 10,
              "EloqStore max inflight write.");
DEFINE_uint32(eloq_store_max_write_batch_pages,
              256,
              "EloqStore max write batch pages.");
DEFINE_uint32(eloq_store_buf_ring_size, 1 << 12, "EloqStore buf ring size.");
DEFINE_uint32(eloq_store_coroutine_stack_size,
              32 * 1024,
              "EloqStore coroutine stack size.");
DEFINE_uint32(eloq_store_num_retained_archives,
              0,
              "EloqStore num retained archives.");
DEFINE_uint32(eloq_store_archive_interval_secs,
              86400,
              "EloqStore archive interval secs.");
DEFINE_uint32(eloq_store_max_archive_tasks,
              256,
              "EloqStore max archive tasks.");
DEFINE_uint32(eloq_store_file_amplify_factor,
              2,
              "EloqStore file amplify factor.");
DEFINE_string(eloq_store_local_space_limit,
              "1TB",
              "EloqStore local space limit.");
DEFINE_uint32(eloq_store_reserve_space_ratio,
              100,
              "EloqStore reserve space ratio.");
DEFINE_bool(eloq_store_prewarm_cloud_cache,
            false,
            "EloqStore prewarm cloud cache during startup.");
DEFINE_uint32(eloq_store_data_page_size, 1 << 12, "EloqStore data page size.");
DEFINE_uint32(eloq_store_pages_per_file_shift,
              11,
              "EloqStore pages per file shift.");
DEFINE_uint32(eloq_store_overflow_pointers, 16, "EloqStore overflow pointers.");
DEFINE_bool(eloq_store_data_append_mode, false, "EloqStore data append mode.");
DEFINE_bool(eloq_store_enable_compression,
            false,
            "EloqStore enable compression.");
DEFINE_string(eloq_store_cloud_store_daemon_ports,
              "5572,5573,5574,5575,5576,5577,5578,5579,5580,5581",
              "EloqStore cloud store daemon ports or URLs (comma-separated)");

namespace EloqDS
{
inline bool CheckCommandLineFlagIsDefault(const char *name)
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

inline bool is_number(const std::string &str)
{
    // regular expression for matching number format
    std::regex pattern("^[0-9]+$");
    return std::regex_match(str, pattern);
}

inline std::string_view remove_last_two(const std::string_view &str)
{
    if (str.length() <= 2)
    {
        return "";
    }
    return std::string_view(str.data(), str.size() - 2);
}

inline std::string_view get_last_two(const std::string_view &str)
{
    if (str.length() <= 2)
    {
        return "";
    }
    return std::string_view(str.data() + str.size() - 2, 2);
}

inline uint64_t unit_num(const std::string_view &unit_str)
{
    if (unit_str == "MB" || unit_str == "mb")
    {
        return 1024 * 1024L;
    }
    else if (unit_str == "GB" || unit_str == "gb")
    {
        return 1024 * 1024 * 1024L;
    }
    else if (unit_str == "TB" || unit_str == "tb")
    {
        return 1024 * 1024 * 1024 * 1024L;
    }

    return 1L;
}

inline bool ends_with(const std::string_view &str,
                      const std::string_view &suffix)
{
    if (str.size() < suffix.size())
    {
        return false;
    }
    if (str.compare(str.size() - suffix.size(), suffix.size(), suffix) != 0)
    {
        return false;
    }

    return true;
}

inline bool is_valid_size(const std::string_view &size_str_v)
{
    bool is_right_end =
        ends_with(size_str_v, "MB") || ends_with(size_str_v, "mb") ||
        ends_with(size_str_v, "GB") || ends_with(size_str_v, "gb") ||
        ends_with(size_str_v, "TB") || ends_with(size_str_v, "tb");

    if (!is_right_end)
    {
        return false;
    }

    std::string num_str;
    num_str = remove_last_two(size_str_v);

    if (!is_number(num_str))
    {
        return false;
    }

    return true;
}

inline uint64_t parse_size(const std::string &size_str)
{
    std::string_view size_str_v(size_str);
    if (!is_valid_size(size_str_v))
    {
        LOG(FATAL) << "Invalid size format: " << size_str;
        return 0;
    }
    std::string_view unit_str = get_last_two(size_str_v);
    uint64_t unit = unit_num(unit_str);
    std::string_view num_str = remove_last_two(size_str_v);
    uint64_t num = std::stoull(std::string(num_str));
    return num * unit;
}

EloqStoreConfig::EloqStoreConfig(const INIReader &config_reader,
                                 const std::string_view base_data_path)
{
    eloqstore_configs_.num_threads =
        !CheckCommandLineFlagIsDefault("eloq_store_worker_num")
            ? FLAGS_eloq_store_worker_num
            : config_reader.GetInteger("store",
                                       "eloq_store_worker_num",
                                       FLAGS_eloq_store_worker_num);
    eloqstore_configs_.num_threads =
        std::max(eloqstore_configs_.num_threads, uint16_t(1));

    std::string storage_path_list =
        !CheckCommandLineFlagIsDefault("eloq_store_data_path_list")
            ? FLAGS_eloq_store_data_path_list
            : config_reader.GetString("store",
                                      "eloq_store_data_path_list",
                                      FLAGS_eloq_store_data_path_list);
    if (!storage_path_list.empty())
    {
        ParseStoragePath(storage_path_list, eloqstore_configs_.store_path);
    }
    else
    {
        eloqstore_configs_.store_path.emplace_back()
            .append(base_data_path)
            .append("/eloqstore_data");
        if (!std::filesystem::exists(eloqstore_configs_.store_path.back()))
        {
            std::filesystem::create_directories(
                eloqstore_configs_.store_path.back());
        }
    }

    eloqstore_configs_.fd_limit =
        !CheckCommandLineFlagIsDefault("eloq_store_open_files_limit")
            ? FLAGS_eloq_store_open_files_limit
            : config_reader.GetInteger("store",
                                       "eloq_store_open_files_limit",
                                       FLAGS_eloq_store_open_files_limit);

    eloqstore_configs_.cloud_store_path =
        !CheckCommandLineFlagIsDefault("eloq_store_cloud_store_path")
            ? FLAGS_eloq_store_cloud_store_path
            : config_reader.GetString("store",
                                      "eloq_store_cloud_store_path",
                                      FLAGS_eloq_store_cloud_store_path);
    LOG_IF(INFO, !eloqstore_configs_.cloud_store_path.empty())
        << "EloqStore cloud store enabled";
    {
        std::string daemon_ports =
            !CheckCommandLineFlagIsDefault(
                "eloq_store_cloud_store_daemon_ports")
                ? FLAGS_eloq_store_cloud_store_daemon_ports
                : config_reader.GetString(
                      "store",
                      "eloq_store_cloud_store_daemon_ports",
                      FLAGS_eloq_store_cloud_store_daemon_ports);
        if (daemon_ports.empty())
        {
            daemon_ports =
                config_reader.GetString("store", "cloud_store_daemon_ports", "");
            if (daemon_ports.empty())
            {
                daemon_ports = config_reader.GetString(
                    "store", "cloud_store_daemon_url", "");
            }
        }
        if (!daemon_ports.empty())
        {
            std::vector<std::string> parsed;
            std::string token;
            std::stringstream ss(daemon_ports);
            while (std::getline(ss, token, ','))
            {
                size_t begin = token.find_first_not_of(" \t\n\r");
                if (begin == std::string::npos)
                {
                    continue;
                }
                size_t end = token.find_last_not_of(" \t\n\r");
                parsed.emplace_back(token.substr(begin, end - begin + 1));
            }
            if (!parsed.empty())
            {
                eloqstore_configs_.cloud_store_daemon_ports = std::move(parsed);
            }
        }
    }
    eloqstore_configs_.data_page_restart_interval =
        !CheckCommandLineFlagIsDefault("eloq_store_data_page_restart_interval")
            ? FLAGS_eloq_store_data_page_restart_interval
            : config_reader.GetInteger(
                  "store",
                  "eloq_store_data_page_restart_interval",
                  FLAGS_eloq_store_data_page_restart_interval);
    eloqstore_configs_.index_page_restart_interval =
        !CheckCommandLineFlagIsDefault("eloq_store_index_page_restart_interval")
            ? FLAGS_eloq_store_index_page_restart_interval
            : config_reader.GetInteger(
                  "store",
                  "eloq_store_index_page_restart_interval",
                  FLAGS_eloq_store_index_page_restart_interval);
    eloqstore_configs_.init_page_count =
        !CheckCommandLineFlagIsDefault("eloq_store_init_page_count")
            ? FLAGS_eloq_store_init_page_count
            : config_reader.GetInteger("store",
                                       "eloq_store_init_page_count",
                                       FLAGS_eloq_store_init_page_count);
    eloqstore_configs_.skip_verify_checksum =
        !CheckCommandLineFlagIsDefault("eloq_store_skip_verify_checksum")
            ? FLAGS_eloq_store_skip_verify_checksum
            : config_reader.GetBoolean("store",
                                       "eloq_store_skip_verify_checksum",
                                       FLAGS_eloq_store_skip_verify_checksum);
    std::string index_buffer_pool_size =
        !CheckCommandLineFlagIsDefault("eloq_store_index_buffer_pool_size")
            ? FLAGS_eloq_store_index_buffer_pool_size
            : config_reader.GetString("store",
                                      "eloq_store_index_buffer_pool_size",
                                      FLAGS_eloq_store_index_buffer_pool_size);
    eloqstore_configs_.index_buffer_pool_size =
        parse_size(index_buffer_pool_size) / eloqstore_configs_.num_threads;
    eloqstore_configs_.manifest_limit =
        !CheckCommandLineFlagIsDefault("eloq_store_manifest_limit")
            ? FLAGS_eloq_store_manifest_limit
            : config_reader.GetInteger("store",
                                       "eloq_store_manifest_limit",
                                       FLAGS_eloq_store_manifest_limit);
    eloqstore_configs_.io_queue_size =
        !CheckCommandLineFlagIsDefault("eloq_store_io_queue_size")
            ? FLAGS_eloq_store_io_queue_size
            : config_reader.GetInteger("store",
                                       "eloq_store_io_queue_size",
                                       FLAGS_eloq_store_io_queue_size);
    eloqstore_configs_.io_queue_size /= eloqstore_configs_.num_threads;
    eloqstore_configs_.max_inflight_write =
        !CheckCommandLineFlagIsDefault("eloq_store_max_inflight_write")
            ? FLAGS_eloq_store_max_inflight_write
            : config_reader.GetInteger("store",
                                       "eloq_store_max_inflight_write",
                                       FLAGS_eloq_store_max_inflight_write);
    eloqstore_configs_.max_inflight_write /= eloqstore_configs_.num_threads;
    eloqstore_configs_.max_write_batch_pages =
        !CheckCommandLineFlagIsDefault("eloq_store_max_write_batch_pages")
            ? FLAGS_eloq_store_max_write_batch_pages
            : config_reader.GetInteger("store",
                                       "eloq_store_max_write_batch_pages",
                                       FLAGS_eloq_store_max_write_batch_pages);
    eloqstore_configs_.buf_ring_size =
        !CheckCommandLineFlagIsDefault("eloq_store_buf_ring_size")
            ? FLAGS_eloq_store_buf_ring_size
            : config_reader.GetInteger("store",
                                       "eloq_store_buf_ring_size",
                                       FLAGS_eloq_store_buf_ring_size);
    eloqstore_configs_.coroutine_stack_size =
        !CheckCommandLineFlagIsDefault("eloq_store_coroutine_stack_size")
            ? FLAGS_eloq_store_coroutine_stack_size
            : config_reader.GetInteger("store",
                                       "eloq_store_coroutine_stack_size",
                                       FLAGS_eloq_store_coroutine_stack_size);
    eloqstore_configs_.num_retained_archives =
        !CheckCommandLineFlagIsDefault("eloq_store_num_retained_archives")
            ? FLAGS_eloq_store_num_retained_archives
            : config_reader.GetInteger("store",
                                       "eloq_store_num_retained_archives",
                                       FLAGS_eloq_store_num_retained_archives);
    eloqstore_configs_.archive_interval_secs =
        !CheckCommandLineFlagIsDefault("eloq_store_archive_interval_secs")
            ? FLAGS_eloq_store_archive_interval_secs
            : config_reader.GetInteger("store",
                                       "eloq_store_archive_interval_secs",
                                       FLAGS_eloq_store_archive_interval_secs);
    eloqstore_configs_.max_archive_tasks =
        !CheckCommandLineFlagIsDefault("eloq_store_max_archive_tasks")
            ? FLAGS_eloq_store_max_archive_tasks
            : config_reader.GetInteger("store",
                                       "eloq_store_max_archive_tasks",
                                       FLAGS_eloq_store_max_archive_tasks);
    eloqstore_configs_.file_amplify_factor =
        !CheckCommandLineFlagIsDefault("eloq_store_file_amplify_factor")
            ? FLAGS_eloq_store_file_amplify_factor
            : config_reader.GetInteger("store",
                                       "eloq_store_file_amplify_factor",
                                       FLAGS_eloq_store_file_amplify_factor);
    std::string local_space_limit =
        !CheckCommandLineFlagIsDefault("eloq_store_local_space_limit")
            ? FLAGS_eloq_store_local_space_limit
            : config_reader.GetString("store",
                                      "eloq_store_local_space_limit",
                                      FLAGS_eloq_store_local_space_limit);
    eloqstore_configs_.local_space_limit = parse_size(local_space_limit);
    eloqstore_configs_.reserve_space_ratio =
        !CheckCommandLineFlagIsDefault("eloq_store_reserve_space_ratio")
            ? FLAGS_eloq_store_reserve_space_ratio
            : config_reader.GetInteger("store",
                                       "eloq_store_reserve_space_ratio",
                                       FLAGS_eloq_store_reserve_space_ratio);
    eloqstore_configs_.prewarm_cloud_cache =
        !CheckCommandLineFlagIsDefault("eloq_store_prewarm_cloud_cache")
            ? FLAGS_eloq_store_prewarm_cloud_cache
            : config_reader.GetBoolean("store",
                                       "eloq_store_prewarm_cloud_cache",
                                       FLAGS_eloq_store_prewarm_cloud_cache);
    eloqstore_configs_.data_page_size =
        !CheckCommandLineFlagIsDefault("eloq_store_data_page_size")
            ? FLAGS_eloq_store_data_page_size
            : config_reader.GetInteger("store",
                                       "eloq_store_data_page_size",
                                       FLAGS_eloq_store_data_page_size);
    eloqstore_configs_.pages_per_file_shift =
        !CheckCommandLineFlagIsDefault("eloq_store_pages_per_file_shift")
            ? FLAGS_eloq_store_pages_per_file_shift
            : config_reader.GetInteger("store",
                                       "eloq_store_pages_per_file_shift",
                                       FLAGS_eloq_store_pages_per_file_shift);
    eloqstore_configs_.overflow_pointers =
        !CheckCommandLineFlagIsDefault("eloq_store_overflow_pointers")
            ? FLAGS_eloq_store_overflow_pointers
            : config_reader.GetInteger("store",
                                       "eloq_store_overflow_pointers",
                                       FLAGS_eloq_store_overflow_pointers);
    eloqstore_configs_.data_append_mode =
        !CheckCommandLineFlagIsDefault("eloq_store_data_append_mode")
            ? FLAGS_eloq_store_data_append_mode
            : config_reader.GetBoolean("store",
                                       "eloq_store_data_append_mode",
                                       FLAGS_eloq_store_data_append_mode);
    eloqstore_configs_.enable_compression =
        !CheckCommandLineFlagIsDefault("eloq_store_enable_compression")
            ? FLAGS_eloq_store_enable_compression
            : config_reader.GetBoolean("store",
                                       "eloq_store_enable_compression",
                                       FLAGS_eloq_store_enable_compression);
}

void EloqStoreConfig::ParseStoragePath(
    const std::string_view storage_path_list,
    std::vector<std::string> &storage_path_vector)
{
    storage_path_vector.clear();
    const char path_delimiter = ',';
    std::string token;
    std::istringstream tokenStream(storage_path_list.data());
    while (std::getline(tokenStream, token, path_delimiter))
    {
        storage_path_vector.emplace_back(token);
    }
}

}  // namespace EloqDS