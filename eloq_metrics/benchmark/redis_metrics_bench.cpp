#include <benchmark/benchmark.h>

#include <memory>

#include "meter.h"
#include "metrics.h"
#include "metrics_registry_impl.h"

using namespace eloq_metrics_app;

static MetricsRegistryImpl::MetricsRegistryResult metrics_registry_result =
    MetricsRegistryImpl::GetRegistry();
static std::unique_ptr<metrics::MetricsRegistry> metrics_registry =
    std::move(metrics_registry_result.metrics_registry_);

static metrics::CommonLabels common_labels{
    {"core_id", "0"}, {"instance", "localhost:8000"}, {"type", "benchmark"}};

static const std::vector<const char *> command_types{"echo",
                                                     "info",
                                                     "cluster",
                                                     "dbsize",
                                                     "time",
                                                     "select",
                                                     "config",
                                                     "ping",
                                                     "eval",
                                                     "dump",
                                                     "restore",
                                                     "get",
                                                     "set",
                                                     "getset",
                                                     "strlen",
                                                     "setnx",
                                                     "psetex",
                                                     "getbit",
                                                     "getrange",
                                                     "substr",
                                                     "incrbyfloat",
                                                     "setbit",
                                                     "setrange",
                                                     "append",
                                                     "setex",
                                                     "rpush",
                                                     "lrange",
                                                     "lpush",
                                                     "lpop",
                                                     "rpop",
                                                     "hset",
                                                     "hget",
                                                     "incr",
                                                     "decr",
                                                     "incrby",
                                                     "decrby",
                                                     "type",
                                                     "del",
                                                     "exists",
                                                     "zadd",
                                                     "zcount",
                                                     "zcard",
                                                     "zrange",
                                                     "zrem",
                                                     "zrangebyscore",
                                                     "zrangebyrank",
                                                     "zrangebylex",
                                                     "zremrangebystore",
                                                     "zremrangebylex",
                                                     "zremrangebyrank",
                                                     "zlexcount",
                                                     "zpopmin",
                                                     "zpopmax",
                                                     "zrandmember",
                                                     "zrank",
                                                     "zrevrank",
                                                     "mget",
                                                     "mset",
                                                     "msetnx",
                                                     "hdel",
                                                     "hlen",
                                                     "hstrlen",
                                                     "hexists",
                                                     "hgetall",
                                                     "hincrby",
                                                     "hmset",
                                                     "hmget",
                                                     "hkeys",
                                                     "hvals",
                                                     "hsetnx",
                                                     "hrandfield",
                                                     "hscan",
                                                     "llen",
                                                     "ltrim",
                                                     "lindex",
                                                     "linsert",
                                                     "lpos",
                                                     "lset",
                                                     "lmove",
                                                     "lrem",
                                                     "lpushx",
                                                     "rpushx",
                                                     "sadd",
                                                     "smembers",
                                                     "srem",
                                                     "flushdb",
                                                     "flushall",
                                                     "scard",
                                                     "sdiff",
                                                     "sdiffstore",
                                                     "sinter",
                                                     "sintercard",
                                                     "sinterstore",
                                                     "sismember",
                                                     "smismember",
                                                     "smove",
                                                     "spop",
                                                     "srandmember",
                                                     "sscan",
                                                     "sunion",
                                                     "sunionstore",
                                                     "sort",
                                                     "sort_ro",
                                                     "dump",
                                                     "restore"};

static const metrics::Map<std::string, std::string> redis_command_access_type{
    {"append", "write"},
    {"bitcount", "read"},
    {"bitfield", "write"},
    {"bitfield_ro", "read"},
    {"bitop", "write"},
    {"bitpos", "read"},
    {"blmove", "write"},
    {"blmpop", "write"},
    {"blpop", "write"},
    {"brpop", "write"},
    {"brpoplpush", "write"},
    {"bzmpop", "write"},
    {"bzpopmax", "write"},
    {"bzpopmin", "write"},
    {"copy", "write"},
    {"dbsize", "read"},
    {"time", "read"},
    {"decr", "write"},
    {"decrby", "write"},
    {"del", "write"},
    {"dump", "read"},
    {"exists", "read"},
    {"expire", "write"},
    {"expireat", "write"},
    {"expiretime", "read"},
    {"flushall", "write"},
    {"flushdb", "write"},
    {"function delete", "write"},
    {"function flush", "write"},
    {"function load", "write"},
    {"function restore", "write"},
    {"geoadd", "write"},
    {"geodist", "read"},
    {"geohash", "read"},
    {"geopos", "read"},
    {"georadius", "write"},
    {"georadius_ro", "read"},
    {"georadiusbymember", "write"},
    {"georadiusbymember_ro", "read"},
    {"geosearch", "read"},
    {"geosearchstore", "write"},
    {"get", "read"},
    {"getbit", "read"},
    {"getdel", "write"},
    {"getex", "write"},
    {"getrange", "read"},
    {"getset", "write"},
    {"hdel", "write"},
    {"hexists", "read"},
    {"hget", "read"},
    {"hgetall", "read"},
    {"hincrby", "write"},
    {"hincrbyfloat", "write"},
    {"hkeys", "read"},
    {"hlen", "read"},
    {"hmget", "read"},
    {"hmset", "write"},
    {"hrandfield", "read"},
    {"hscan", "read"},
    {"hset", "write"},
    {"hsetnx", "write"},
    {"hstrlen", "read"},
    {"hvals", "read"},
    {"incr", "write"},
    {"incrby", "write"},
    {"incrbyfloat", "write"},
    {"keys", "read"},
    {"lcs", "read"},
    {"lindex", "read"},
    {"linsert", "write"},
    {"llen", "read"},
    {"lmove", "write"},
    {"lmpop", "write"},
    {"lolwut", "read"},
    {"lpop", "write"},
    {"lpos", "read"},
    {"lpush", "write"},
    {"lpushx", "write"},
    {"lrange", "read"},
    {"lrem", "write"},
    {"lset", "write"},
    {"ltrim", "write"},
    {"memory usage", "read"},
    {"mget", "read"},
    {"migrate", "write"},
    {"move", "write"},
    {"mset", "write"},
    {"msetnx", "write"},
    {"object encoding", "read"},
    {"object freq", "read"},
    {"object idletime", "read"},
    {"object refcount", "read"},
    {"persist", "write"},
    {"pexpire", "write"},
    {"pexpireat", "write"},
    {"pexpiretime", "read"},
    {"pfadd", "write"},
    {"pfcount", "read"},
    {"pfdebug", "write"},
    {"pfmerge", "write"},
    {"psetex", "write"},
    {"pttl", "read"},
    {"randomkey", "read"},
    {"rename", "write"},
    {"renamenx", "write"},
    {"restore", "write"},
    {"restore-asking", "write"},
    {"rpop", "write"},
    {"rpoplpush", "write"},
    {"rpush", "write"},
    {"rpushx", "write"},
    {"sadd", "write"},
    {"scan", "read"},
    {"scard", "read"},
    {"sdiff", "read"},
    {"sdiffstore", "write"},
    {"set", "write"},
    {"setbit", "write"},
    {"setex", "write"},
    {"setnx", "write"},
    {"setrange", "write"},
    {"sinter", "read"},
    {"sintercard", "read"},
    {"sinterstore", "write"},
    {"sismember", "read"},
    {"smembers", "read"},
    {"smismember", "read"},
    {"smove", "write"},
    {"sort", "write"},
    {"sort_ro", "read"},
    {"spop", "write"},
    {"srandmember", "read"},
    {"srem", "write"},
    {"sscan", "read"},
    {"strlen", "read"},
    {"substr", "read"},
    {"sunion", "read"},
    {"sunionstore", "write"},
    {"swapdb", "write"},
    {"touch", "read"},
    {"ttl", "read"},
    {"type", "read"},
    {"unlink", "write"},
    {"xack", "write"},
    {"xadd", "write"},
    {"xautoclaim", "write"},
    {"xclaim", "write"},
    {"xdel", "write"},
    {"xgroup create", "write"},
    {"xgroup createconsumer", "write"},
    {"xgroup delconsumer", "write"},
    {"xgroup destroy", "write"},
    {"xgroup setid", "write"},
    {"xinfo consumers", "read"},
    {"xinfo groups", "read"},
    {"xinfo stream", "read"},
    {"xlen", "read"},
    {"xpending", "read"},
    {"xrange", "read"},
    {"xread", "read"},
    {"xreadgroup", "write"},
    {"xrevrange", "read"},
    {"xsetid", "write"},
    {"xtrim", "write"},
    {"zadd", "write"},
    {"zcard", "read"},
    {"zcount", "read"},
    {"zdiff", "read"},
    {"zdiffstore", "write"},
    {"zincrby", "write"},
    {"zinter", "read"},
    {"zintercard", "read"},
    {"zinterstore", "write"},
    {"zlexcount", "read"},
    {"zmpop", "write"},
    {"zmscore", "read"},
    {"zpopmax", "write"},
    {"zpopmin", "write"},
    {"zrandmember", "read"},
    {"zrange", "read"},
    {"zrangebylex", "read"},
    {"zrangebyscore", "read"},
    {"zrangestore", "write"},
    {"zrank", "read"},
    {"zrem", "write"},
    {"zremrangebylex", "write"},
    {"zremrangebyrank", "write"},
    {"zremrangebyscore", "write"},
    {"zrevrange", "read"},
    {"zrevrangebylex", "read"},
    {"zrevrangebyscore", "read"},
    {"zrevrank", "read"},
    {"zscan", "read"},
    {"zscore", "read"},
    {"zunion", "read"},
    {"zunionstore", "write"},
};

// measuring the time required to obtain a time point
static void BM_get_time_point(benchmark::State &state)
{
    while (state.KeepRunning())
    {
        auto time_start = metrics::Clock::now();
    }
}

static void BM_redis_command_counter(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);
    metrics::Name name{"counter"};
    for (const auto &cmd : command_types)
    {
        std::string access_type = "none";
        auto pair = redis_command_access_type.find(cmd);
        if (pair != redis_command_access_type.end())
        {
            access_type = pair->second;
        }
        meter->Register(name,
                        metrics::Type::Counter,
                        {{"type", {cmd}}, {"access_type", {access_type}}});
    }
    while (state.KeepRunning())
    {
        std::string access_type = "none";
        auto pair = redis_command_access_type.find("set");
        if (pair != redis_command_access_type.end())
        {
            access_type = pair->second;
        }
        meter->Collect(name, 1, "set", access_type);
    }
}

static void BM_redis_command_histogram(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);

    const metrics::Name name{"histogram"};
    for (const auto &cmd : command_types)
    {
        std::string access_type = "none";
        auto pair = redis_command_access_type.find(cmd);
        if (pair != redis_command_access_type.end())
        {
            access_type = pair->second;
        }
        meter->Register(name,
                        metrics::Type::Histogram,
                        {{"type", {cmd}}, {"access_type", {access_type}}});
    }

    while (state.KeepRunning())
    {
        std::string access_type = "none";
        auto pair = redis_command_access_type.find("set");
        if (pair != redis_command_access_type.end())
        {
            access_type = pair->second;
        }
        meter->Collect(name, 1, "set", access_type);
    }
}

BENCHMARK(BM_get_time_point)
    ->Iterations(1000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_redis_command_counter)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_redis_command_histogram)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();
