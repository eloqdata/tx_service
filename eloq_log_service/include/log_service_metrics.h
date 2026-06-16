#pragma once

#include <atomic>

namespace metrics
{
inline std::atomic_bool enable_log_service_metrics{false};
}  // namespace metrics
