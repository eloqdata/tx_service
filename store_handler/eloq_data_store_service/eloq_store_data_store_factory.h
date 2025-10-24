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

#include <memory>
#include <vector>

#include "data_store_factory.h"
#include "eloq_store_data_store.h"

namespace EloqDS
{
class EloqStoreDataStoreFactory : public DataStoreFactory
{
public:
    explicit EloqStoreDataStoreFactory(EloqStoreConfig &&configs)
        : eloq_store_configs_(std::move(configs))
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true) override
    {
        auto ds =
            std::make_unique<EloqStoreDataStore>(shard_id, data_store_service);
        ds->Initialize();
        if (start_db)
        {
            if (!ds->StartDB())
            {
                return nullptr;
            }
        }
        return ds;
    }

private:
    const EloqStoreConfig eloq_store_configs_;

    friend class EloqStoreDataStore;
};
}  // namespace EloqDS
