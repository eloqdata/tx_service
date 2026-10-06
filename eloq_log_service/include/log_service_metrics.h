#pragma once

#include <atomic>

#include "metrics.h"

namespace metrics
{
inline std::atomic_bool enable_log_service_metrics{false};

/**
 * The log group's arrival_complete_ts, in microseconds since the epoch: every
 * record at or below it has arrived, so change data capture can emit up to it
 * (EloqDoc docs/cdc-change-stream.md §4.2). Set on each replica when it
 * advances; how far it trails the wall clock is the change streams' lag floor.
 */
inline const Name NAME_CDC_ARRIVAL_COMPLETE_TS{"cdc_arrival_complete_ts"};

/**
 * The CdcScan requests parked on the log group's leader, waiting for
 * arrival_complete_ts to advance or for their wait to end; at most
 * cdc_scan_max_parked.
 */
inline const Name NAME_CDC_SCAN_PARKED_REQUESTS{"cdc_scan_parked_requests"};
}  // namespace metrics
