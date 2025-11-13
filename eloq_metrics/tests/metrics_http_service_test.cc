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
#include <catch2/catch_all.hpp>
#include <iostream>
#include <vector>

#include "curl/curl.h"
#include "metrics_manager.h"
#include "prometheus/exposer.h"

static size_t write_call_back(void *contents,
                              size_t size,
                              size_t nmemb,
                              void *userp)
{
    ((std::string *) userp)->append((char *) contents, size * nmemb);
    return size * nmemb;
}

SCENARIO("eloq_metrics http service (success)", "[PushBasedMetricsHttpService]")
{
    auto mgr_result = metrics::MetricsMgr::GetMetricMgrInstance();
    REQUIRE(mgr_result.not_ok_ == nullptr);
    WHEN("default port (18081) available.")
    {
        std::string rsp_buffer;
        curl_global_init(CURL_GLOBAL_ALL);
        CURL *curl = curl_easy_init();
        REQUIRE(curl);
        curl_easy_setopt(
            curl, CURLOPT_URL, "http://0.0.0.0:18081/eloq_metrics");
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_call_back);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rsp_buffer);
        CURLcode curl_response = curl_easy_perform(curl);
        THEN("access to eloq_metrics http service return 200.")
        {
            REQUIRE(curl_response == CURLE_OK);
            long http_status;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
            INFO("/eloq_metrics response \n" << rsp_buffer);
            REQUIRE(http_status == 200);
            curl_easy_cleanup(curl);
        }
    }
}

SCENARIO("eloq_metrics http service (port conflict)",
         "[.][MetricsHttpPortAlreadyBind]")
{
    WHEN("port 18081 already bind")
    {
        prometheus::Exposer exposer{"0.0.0.0:18081"};
        std::vector<int> bind_port_vec = exposer.GetListeningPorts();
        size_t bind_port;
        std::for_each(bind_port_vec.begin(),
                      bind_port_vec.end(),
                      [&bind_port](int e)
                      {
                          std::cout << "bind port: " << e << std::endl;
                          bind_port = e;
                      });
        REQUIRE(bind_port == 18081);
        THEN("port conflict MetricsMgr init failure")
        {
            auto mgr_result = metrics::MetricsMgr::GetMetricMgrInstance();
            std::string err_msg(mgr_result.not_ok_);
            INFO("MetricsMgrResult ErrMsg: " << err_msg);
            REQUIRE(mgr_result.not_ok_ != nullptr);
            REQUIRE(mgr_result.mgr_ == nullptr);
        }
    }
}
