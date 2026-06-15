#pragma once

#include <assert.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <regex>
#include <string>
#include <string_view>

#if ((defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS)) && \
     !defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_CLOUD 1
#endif

#if (defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS) || \
     defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_ALL 1
#endif

namespace txlog
{
inline bool is_number(const std::string &str)
{
    // regular expression for matching number format
    std::regex pattern("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
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

inline bool ends_with(const std::string_view &str,
                      const std::string_view &suffix)
{
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
        LOG(WARNING) << "size string should end with MB, GB or TB";
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

inline uint64_t parse_size(const std::string &size_str)
{
    std::string_view size_str_v(size_str);
    if (!is_valid_size(size_str_v))
    {
        LOG(ERROR) << "Invalid size string: " << size_str;
        std::abort();
    }
    std::string_view unit_str = get_last_two(size_str_v);
    uint64_t unit = unit_num(unit_str);
    std::string_view num_str = remove_last_two(size_str_v);
    uint64_t num = std::stol(std::string(num_str));
    return num * unit;
}

inline std::vector<std::string> split(const std::string &str,
                                      const std::string &delim)
{
    std::vector<std::string> tokens;
    size_t prev = 0, pos = 0;
    do
    {
        pos = str.find(delim, prev);
        if (pos == std::string::npos)
            pos = str.length();
        std::string token = str.substr(prev, pos - prev);
        if (!token.empty())
            tokens.push_back(token);
        prev = pos + delim.length();
    } while (pos < str.length() && prev < str.length());
    return tokens;
}

inline std::string MakeCloudManifestFile(const std::string &dbname,
                                         const std::string &cookie)
{
    return cookie.empty() ? (dbname + "/CLOUDMANIFEST")
                          : (dbname + "/CLOUDMANIFEST-" + cookie);
}

inline bool BthreadCondWaitFor(bthread::Mutex &bmutex,
                               bthread::ConditionVariable &cv,
                               int64_t timeout_us,
                               std::function<bool()> stop_waiting)
{
    std::unique_lock<bthread::Mutex> lk(bmutex);
    while (!stop_waiting())
    {
        if (cv.wait_for(lk, timeout_us) == ETIMEDOUT)
        {
            return stop_waiting();
        }
    }
    return true;
}

inline std::string FormatSize(uint64_t size)
{
    std::string size_str;
    if (size > 1024 * 1024 * 1024)
    {
        size_str = std::to_string(size / (1024 * 1024 * 1024)) + " GB";
    }
    else
    {
        size_str = std::to_string(size / (1024 * 1024)) + " MB";
    }
    return size_str;
}

inline bool IsDefaultGFlag(const char *name)
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
}  // namespace txlog