/**
 *    Copyright (C) 2026 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 */

#pragma once

#include <gflags/gflags_declare.h>

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB) ||                                \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                       \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
DECLARE_bool(ignore_redis_ttl);
#endif

namespace EloqDS
{
/**
 * Returns whether RocksDB-backed EloqDSS should preserve and expose expired
 * EloqKV records for diagnostics. Non-RocksDB builds always return false.
 */
inline bool IgnoreRedisTTL()
{
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB) ||                                \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                       \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
    return FLAGS_ignore_redis_ttl;
#else
    return false;
#endif
}
}  // namespace EloqDS
