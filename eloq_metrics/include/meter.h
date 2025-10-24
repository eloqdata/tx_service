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

#include <cassert>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "metrics.h"

namespace metrics
{
using MeterKey = std::size_t;
/**
 * @class Meter
 * @brief A class for managing metrics with labels and collecting metric values.
 *
 * The Meter class provides a convenient way to register metrics with labels and
 * collect metric values. It supports associating multiple labels with each
 * metric, allowing for fine-grained categorization of data. The class interacts
 * with a MetricsRegistry instance to register and collect metrics. The Meter
 * class uses a mapping between meter keys (constructed from metric names and
 * labels) and metric keys to efficiently collect metric values.
 *
 * Usage:
 * 1. Create an instance of the Meter class by providing a pointer to a
 * MetricsRegistry object and a core identifier.
 * 2. Register metrics using the Register() function, specifying the metric
 * name, type, and label groups. Label groups consist of a label name and a
 * vector of label types. The Meter class automatically adds a "core_id" label
 * to every metric using the provided core identifier. The Register() function
 * generates the cartesian product of label groups and registers the metric for
 * each unique label combination.
 * 3. Collect metric values using the Collect() function.
 *    Specify the metric name, value, and label values as variadic template
 * arguments. The Collect() function retrieves the metric key from the
 * meter_to_metric_map_ and uses the MetricsRegistry to collect the metric
 * value. If the meter key is not found in the map, an assertion failure occurs.
 * 4. Optionally, use the CollectDuration() function to collect duration
 * metrics. Provide the metric name, start time, and label values to calculate
 * the duration and collect the metric.
 *
 * Example:
 * ```
 * MetricsRegistry metrics_registry;
 * Meter meter(&metrics_registry, "core1");
 *
 * // Register a metric with two label groups
 * meter.Register("requests_total", Type::Counter, {
 *     {"method", {"GET", "POST", "PUT"}},
 *     {"status_code", {"200", "404", "500"}}
 * });
 *
 * // Collect metric values
 *
 * // Increment the metric with label values "GET" and "200" by 1
 * meter.Collect("requests_total", 1, "GET", "200");
 * // Increment the metric with label values "POST" and "404" by 2
 * meter.Collect("requests_total", 2, "POST", "404");
 *
 * // Collect duration metric
 * auto start_time = std::chrono::steady_clock::now();
 * ...do some operations
 * // Collect the duration of the operation with label values "GET" and "200"
 * meter.CollectDuration("request_duration", start_time, "GET", "200");
 * ```
 */
using LabelGroup = std::pair<std::string, std::vector<std::string>>;
class Meter
{
public:
    /**
     * @brief Deleted default constructor.
     */
    Meter() = delete;

    /**
     * @brief Deleted copy constructor.
     * @param other The Meter object to be copied.
     */
    Meter(const Meter &other) = delete;

    /**
     * @brief Deleted move constructor.
     * @param other The Meter object to be moved.
     */
    Meter(Meter &&other) noexcept = delete;

    Meter(MetricsRegistry *metrics_registry, const CommonLabels &common_labels)
        : metrics_registry_(metrics_registry)
    {
        for (const auto &[k, v] : common_labels)
        {
            common_label_groups_.push_back({std::string(k), {std::string(v)}});
        }
    }

    /**
     * @brief Default destructor.
     */
    virtual ~Meter() noexcept = default;

    /**
     * @brief Deleted copy assignment operator.
     * @param other The Meter object to be copied.
     * @return Reference to the current Meter object.
     */
    Meter &operator=(const Meter &other) = delete;

    /**
     * @brief Deleted move assignment operator.
     * @param other The Meter object to be moved.
     * @return Reference to the current Meter object.
     */
    Meter &operator=(Meter &&other) noexcept = delete;

    /**
     * @brief Registers a metric with labels.
     * @param name The name of the metric.
     * @param type The type of the metric.
     * @param label_groups The deque of label groups. Each group contains a
     * label name and a vector of label types.
     *
     * This function registers a metric with the provided name and type, along
     * with the specified label groups. The label groups represent different
     * sets of labels that can be associated with the metric. The core_id label
     * is automatically added to every metric, using the provided core_id value.
     * The function generates the cartesian product of label groups and
     * registers the metric for each unique label combination.
     *
     * CAUTION: Remember that every unique combination of key-value label pairs
     * represents a new time series, which can dramatically increase the amount
     * of data stored. Do not use labels to store dimensions with high
     * cardinality (many different label values), such as user IDs, email
     * addresses, or other unbounded sets of values.
     */
    void Register(const Name &name,
                  const Type &type,
                  std::vector<LabelGroup> &&dynamic_label_groups = {})
    {
        std::vector<Labels> all_labels;

        std::vector<LabelGroup> all_label_groups{};
        all_label_groups.insert(all_label_groups.end(),
                                common_label_groups_.begin(),
                                common_label_groups_.end());
        all_label_groups.insert(all_label_groups.end(),
                                dynamic_label_groups.begin(),
                                dynamic_label_groups.end());
        GenerateCartesianProduct(all_label_groups, 0, Labels(), all_labels);
        auto name_str = name.GetName();
        for (const auto &labels : all_labels)
        {
            auto metric_key =
                metrics_registry_->Register(name_str, type, labels);
            auto meter_key = Hash(name_str);
            for (size_t i = common_label_groups_.size(); i < labels.size(); ++i)
            {
                const auto &pair = labels[i];
                meter_key = Hash(meter_key, pair.second);
            }
            meter_to_metric_map_.emplace(std::move(meter_key),
                                         std::move(metric_key));
        }
    };

    /**
     * @brief Collects a metric value with labels.
     * @param name The name of the metric.
     * @param value The value of the metric.
     * @param label_types The variadic template parameter representing the label
     * values.
     *
     * This function collects a metric value with the provided name, value, and
     * labels. The labels are specified as variadic template arguments, and
     * their values are concatenated to form a unique meter key. The function
     * retrieves the corresponding metric key from the meter_to_metric_map_ and
     * collects the metric using the MetricsRegistry. If the meter key is not
     * found in the map, an assertion failure occurs.
     */
    template <typename... LabelTypes>
    void Collect(const Name &name,
                 const Value &value,
                 const LabelTypes &...label_types)
    {
        auto meter_key = Hash(name.GetName());
        ((meter_key = Hash(meter_key, label_types)), ...);

        auto it = meter_to_metric_map_.find(meter_key);
        assert(it != meter_to_metric_map_.end());

        // To avoid db crash resulting from a meter key missing
        if (it != meter_to_metric_map_.end())
        {
            metrics_registry_->Collect(meter_to_metric_map_[meter_key], value);
        }
    }

    /**
     * @brief Collects a metric value with labels.
     * @param name The name of the metric.
     * @param value The value of the metric as a double.
     * @param label_types The variadic template parameter representing the label
     * values.
     *
     * This function is an overload of the Collect() function for metrics with
     * double values. It converts the double value to a Value object and calls
     * the Collect() function with the appropriate parameters.
     */
    template <typename... LabelTypes>
    void Collect(const Name &name,
                 double value,
                 const LabelTypes &...label_types)
    {
        Collect(name, Value(value), label_types...);
    }

    /**
     * @brief Collects a metric value with labels.
     * @param name The name of the metric.
     * @param inc_value The incremental/decremental value of the metric.
     * @param label_types The variadic template parameter representing the label
     * values.
     *
     * This function is an overload of the Collect() function for metrics with
     * incremental/decremental values. It wraps the inc_value in a Value object
     * and calls the Collect() function with the appropriate parameters.
     */
    template <typename... LabelTypes>
    void Collect(const Name &name,
                 const Value::IncDecValue &inc_value,
                 const LabelTypes &...label_types)
    {
        Collect(name, Value(inc_value), label_types...);
    }

    /**
     * @brief Collects duration metrics based on the provided start time and
     * labels.
     * @tparam LabelTypes Variadic template parameter representing the types of
     * the labels.
     * @param start_time The starting time point for calculating the duration.
     * @param label_types The label values used to construct the meter key.
     *
     * This method is a convenience function that calculates the duration from
     * the provided start time to the current time using the Clock object, and
     * then calls the Collect function to collect the duration metric. The
     * duration is collected using the provided labels and the meter key
     * constructed from the label types.
     */
    template <typename... LabelTypes>
    void CollectDuration(const Name &name,
                         const TimePoint &start_time,
                         const LabelTypes &...label_types)
    {
        Collect(name,
                std::chrono::duration_cast<std::chrono::microseconds>(
                    Clock::now() - start_time)
                    .count(),
                label_types...);
    };

private:
    /**
     * @brief Generates the cartesian product of label groups.
     * @param label_groups A deque of LabelGroup objects representing different
     * groups of labels.
     * @param index The current index in the label_groups deque.
     * @param current_labels The labels generated so far in the recursive
     * process.
     * @param all_labels The vector to store all generated label combinations.
     *
     * This recursive function generates the cartesian product of label groups.
     * Starting from the first label group, it iterates over each label type in
     * the group and appends it to the current labels. Then, it recursively
     * calls itself with the next index to process the next label group. Once it
     * reaches the last label group, it adds the current set of labels to the
     * vector of all labels. Finally, it backtracks by removing the last added
     * label pair to explore other combinations.
     */
    void GenerateCartesianProduct(std::vector<LabelGroup> &label_groups,
                                  size_t index,
                                  Labels current_labels,
                                  std::vector<Labels> &all_labels)
    {
        // Base case: If the index is equal to the number of label_groups, we
        // have processed all label groups,  and can add the current set of
        // labels to the vector of all labels.
        if (index == label_groups.size())
        {
            all_labels.push_back(std::move(current_labels));
            return;
        }

        // Get the label group at the current index
        const auto &group = label_groups[index];
        const auto &label_name = group.first;
        const auto &all_label_types = group.second;

        // Iterate over each key-value pair in the label group
        for (const auto &label_type : all_label_types)
        {
            current_labels.push_back(std::make_pair(label_name, label_type));
            // Recursively call the function with the next index to process
            // the next label group.
            GenerateCartesianProduct(
                label_groups, index + 1, current_labels, all_labels);
            // Remove the last added label pair to backtrack and explore
            // other combinations.
            current_labels.pop_back();
        }
    };

    std::size_t Hash(const std::string_view &s) const
    {
        return std::hash<std::string_view>{}(s);
    }

    MeterKey Hash(MeterKey current_hash, const std::string_view &s) const
    {
        auto new_hash = std::hash<std::string_view>{}(s);
        return current_hash ^ (0x9e3779b9 + new_hash + (current_hash << 6) +
                               (current_hash >> 2));
    };

    MetricsRegistry *metrics_registry_{nullptr};
    std::vector<LabelGroup> common_label_groups_{};
    Map<MeterKey, MetricKey> meter_to_metric_map_{};
};
}  // namespace metrics
