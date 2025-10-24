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
#include "metrics_manager.h"

#include <catch2/catch_all.hpp>

SCENARIO("MetricsMgr call getInstance only once", "[MetricsManagerInitSuccess]")
{
    WHEN("MetricsMgr use default metrics port and url")
    {
        auto mgr_1 = metrics::MetricsMgr::GetMetricMgrInstance();
        THEN("metrics collector init and open")
        {
            REQUIRE(mgr_1.not_ok_ == nullptr);
        }
    }
}

SCENARIO("MetricsMgr call getInstance several times",
         "[MetricsManagerObjUnique]")
{
    auto mgr_res_1 = metrics::MetricsMgr::GetMetricMgrInstance();
    auto mgr_res_2 = metrics::MetricsMgr::GetMetricMgrInstance();
    WHEN("MetricsMgr GetMetricMgrInstance() call twice")
    {
        THEN("Two MetricsMgr instance equal")
        {
            INFO("check MetricsMgrResult");
            REQUIRE(mgr_res_1.not_ok_ == mgr_res_2.not_ok_);
            metrics::MetricsMgr &mgr_1 = *mgr_res_1.mgr_;
            metrics::MetricsMgr &mgr_2 = *mgr_res_2.mgr_;
            auto mgr_instance_equals = mgr_1 == mgr_2;
            REQUIRE(mgr_instance_equals);
        }
    }
}
