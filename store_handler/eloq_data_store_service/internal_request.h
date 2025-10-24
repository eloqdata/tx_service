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

#include <brpc/closure_guard.h>

#include <string>
#include <utility>
#include <vector>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "object_pool.h"

namespace EloqDS
{
class WriteRecordsRequest : public Poolable
{
public:
    WriteRecordsRequest() = default;
    WriteRecordsRequest(const WriteRecordsRequest &other) = delete;
    WriteRecordsRequest &operator=(const WriteRecordsRequest &other) = delete;

    virtual size_t RecordsCount() const = 0;

    virtual const std::string_view GetTableName() const = 0;

    virtual const std::string_view GetKeyPart(size_t index) const = 0;

    virtual uint16_t PartsCountPerKey() const = 0;

    virtual int32_t GetPartitionId() const = 0;

    virtual const std::string_view GetRecordPart(size_t index) const = 0;

    virtual uint16_t PartsCountPerRecord() const = 0;

    virtual uint64_t GetRecordTs(size_t index) const = 0;

    virtual uint64_t GetRecordTtl(size_t index) const = 0;

    virtual WriteOpType KeyOpType(size_t index) const = 0;

    virtual bool SkipWal() const = 0;

    virtual void SetFinish(const remote::CommonResult &result) = 0;
};

class WriteRecordsRpcRequest : public WriteRecordsRequest
{
public:
    WriteRecordsRpcRequest() = default;
    WriteRecordsRpcRequest(const WriteRecordsRpcRequest &other) = delete;
    WriteRecordsRpcRequest &operator=(const WriteRecordsRpcRequest &other) =
        delete;

    void Clear() override
    {
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const remote::BatchWriteRecordsRequest *req,
               remote::BatchWriteRecordsResponse *resp,
               google::protobuf::Closure *done)
    {
        data_store_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    size_t RecordsCount() const override
    {
        return req_->items_size();
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name();
    }

    const std::string_view GetKeyPart(size_t index) const override
    {
        return req_->items(index).key();
    }

    uint16_t PartsCountPerKey() const override
    {
        return 1;
    }

    int32_t GetPartitionId() const override
    {
        return req_->partition_id();
    }

    const std::string_view GetRecordPart(size_t index) const override
    {
        return req_->items(index).value();
    }

    uint16_t PartsCountPerRecord() const override
    {
        return 1;
    }

    uint64_t GetRecordTs(size_t index) const override
    {
        return req_->items(index).ts();
    }

    uint64_t GetRecordTtl(size_t index) const override
    {
        return req_->items(index).ttl();
    }

    // virtual bool IsDeleted(size_t index) const = 0;
    WriteOpType KeyOpType(size_t index) const override
    {
        if (req_->items(index).op_type() == remote::WriteOpType::Delete)
        {
            return WriteOpType::DELETE;
        }
        else
        {
            assert(req_->items(index).op_type() == remote::WriteOpType::Put);
            return WriteOpType::PUT;
        }
    }

    bool SkipWal() const override
    {
        return req_->skip_wal();
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        // Set error code and error message
        resp_->mutable_result()->set_error_code(result.error_code());
        resp_->mutable_result()->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *data_store_service_{nullptr};
    const remote::BatchWriteRecordsRequest *req_{nullptr};
    remote::BatchWriteRecordsResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class WriteRecordsLocalRequest : public WriteRecordsRequest
{
public:
    WriteRecordsLocalRequest() = default;
    WriteRecordsLocalRequest(const WriteRecordsLocalRequest &other) = delete;
    WriteRecordsLocalRequest &operator=(const WriteRecordsLocalRequest &other) =
        delete;

    void Clear() override
    {
        table_name_ = "";
        partition_id_ = 0;
        key_parts_ = nullptr;
        record_parts_ = nullptr;
        ts_ = nullptr;
        ttl_ = nullptr;
        op_types_ = nullptr;
        skip_wal_ = false;
        result_ = nullptr;
        done_ = nullptr;
        parts_cnt_per_key_ = 1;
        parts_cnt_per_record_ = 1;
    }

    void Reset(DataStoreService *ds_service,
               std::string_view table_name,
               int32_t partition_id,
               const std::vector<std::string_view> &key_parts,
               const std::vector<std::string_view> &record_parts,
               const std::vector<uint64_t> &ts,
               const std::vector<uint64_t> &ttl,
               const std::vector<WriteOpType> &op_types,
               bool skip_wal,
               remote::CommonResult &result,
               google::protobuf::Closure *done,
               const uint16_t parts_cnt_per_key,
               const uint16_t parts_cnt_per_record)
    {
        data_store_service_ = ds_service;
        table_name_ = table_name;
        partition_id_ = partition_id;
        key_parts_ = &key_parts;
        record_parts_ = &record_parts;
        ts_ = &ts;
        ttl_ = &ttl;
        op_types_ = &op_types;
        skip_wal_ = skip_wal;
        result_ = &result;
        done_ = done;
        parts_cnt_per_key_ = parts_cnt_per_key;
        parts_cnt_per_record_ = parts_cnt_per_record;
    }

    size_t RecordsCount() const override
    {
        assert(key_parts_->size() % parts_cnt_per_key_ == 0);
        assert(record_parts_->size() % parts_cnt_per_record_ == 0);
        assert(key_parts_->size() / parts_cnt_per_key_ ==
               record_parts_->size() / parts_cnt_per_record_);
        assert(ts_->size() == key_parts_->size() / parts_cnt_per_key_);
        assert(ttl_->size() == key_parts_->size() / parts_cnt_per_key_);
        return key_parts_->size() / parts_cnt_per_key_;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    const std::string_view GetKeyPart(size_t index) const override
    {
        return key_parts_->at(index);
    }

    uint16_t PartsCountPerKey() const override
    {
        return parts_cnt_per_key_;
    }

    int32_t GetPartitionId() const override
    {
        return partition_id_;
    }

    const std::string_view GetRecordPart(size_t index) const override
    {
        return record_parts_->at(index);
    }

    uint16_t PartsCountPerRecord() const override
    {
        return parts_cnt_per_record_;
    }

    uint64_t GetRecordTs(size_t index) const override
    {
        return ts_->at(index);
    }

    uint64_t GetRecordTtl(size_t index) const override
    {
        return ttl_->at(index);
    }

    WriteOpType KeyOpType(size_t index) const override
    {
        return op_types_->at(index);
    }

    bool SkipWal() const override
    {
        return skip_wal_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        // Set error code and error message
        result_->set_error_code(result.error_code());
        result_->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *data_store_service_{nullptr};
    std::string_view table_name_;
    int32_t partition_id_;
    const std::vector<std::string_view> *key_parts_{nullptr};
    const std::vector<std::string_view> *record_parts_{nullptr};
    const std::vector<uint64_t> *ts_{nullptr};
    const std::vector<uint64_t> *ttl_{nullptr};
    const std::vector<WriteOpType> *op_types_{nullptr};
    bool skip_wal_{false};
    remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
    uint16_t parts_cnt_per_key_{1};
    uint16_t parts_cnt_per_record_{1};
};

class FlushDataRequest : public Poolable
{
public:
    FlushDataRequest() = default;
    FlushDataRequest(const FlushDataRequest &other) = delete;
    FlushDataRequest &operator=(const FlushDataRequest &other) = delete;

    virtual ~FlushDataRequest() = default;

    // parameters in
    virtual const std::vector<std::string> &GetKvTableNames() const = 0;

    // finish
    virtual void SetFinish(const remote::CommonResult &result) = 0;
};

class FlushDataRpcRequest : public FlushDataRequest
{
public:
    FlushDataRpcRequest() = default;
    FlushDataRpcRequest(const FlushDataRpcRequest &other) = delete;
    FlushDataRpcRequest &operator=(const FlushDataRpcRequest &other) = delete;

    void Clear() override
    {
        kv_table_names_.clear();
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const remote::FlushDataRequest *req,
               remote::FlushDataResponse *resp,
               google::protobuf::Closure *done)
    {
        data_store_service_ = ds_service;
        for (int idx = 0; idx < req->kv_table_name_size(); ++idx)
        {
            kv_table_names_.push_back(req->kv_table_name(idx));
        }

        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    const std::vector<std::string> &GetKvTableNames() const override
    {
        return kv_table_names_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);

        ::EloqDS::remote::CommonResult *res = resp_->mutable_result();
        res->set_error_code(result.error_code());
        res->set_error_msg(result.error_msg());
    }

    const remote::FlushDataRequest *GetRequest()
    {
        return req_;
    }

    remote::FlushDataResponse *GetResponse()
    {
        return resp_;
    }

private:
    DataStoreService *data_store_service_{nullptr};
    std::vector<std::string> kv_table_names_;
    const remote::FlushDataRequest *req_{nullptr};
    remote::FlushDataResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class FlushDataLocalRequest : public FlushDataRequest
{
public:
    FlushDataLocalRequest() = default;
    FlushDataLocalRequest(const FlushDataLocalRequest &other) = delete;
    FlushDataLocalRequest &operator=(const FlushDataLocalRequest &other) =
        delete;

    void Clear() override
    {
        kv_table_names_ = nullptr;
        result_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const std::vector<std::string> *kv_table_names,
               remote::CommonResult &result,
               google::protobuf::Closure *done)
    {
        data_store_service_ = ds_service;
        kv_table_names_ = kv_table_names;
        result_ = &result;
        done_ = done;
    }

    const std::vector<std::string> &GetKvTableNames() const override
    {
        return *kv_table_names_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        result_->set_error_code(result.error_code());
        result_->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *data_store_service_{nullptr};
    const std::vector<std::string> *kv_table_names_{nullptr};
    remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class DeleteRangeRequest : public Poolable
{
public:
    DeleteRangeRequest() = default;
    DeleteRangeRequest(const DeleteRangeRequest &other) = delete;
    DeleteRangeRequest &operator=(const DeleteRangeRequest &other) = delete;

    virtual ~DeleteRangeRequest() = default;

    // parameters in
    virtual const std::string_view GetTableName() const = 0;
    virtual uint32_t GetPartitionId() const = 0;
    virtual const std::string_view GetStartKey() const = 0;
    virtual const std::string_view GetEndKey() const = 0;
    virtual bool SkipWal() const = 0;

    // finish
    virtual void SetFinish(const remote::CommonResult &result) = 0;
};

class DeleteRangeRpcRequest : public DeleteRangeRequest
{
public:
    DeleteRangeRpcRequest() = default;
    DeleteRangeRpcRequest(const DeleteRangeRpcRequest &other) = delete;
    DeleteRangeRpcRequest &operator=(const DeleteRangeRpcRequest &other) =
        delete;

    void Clear() override
    {
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const remote::DeleteRangeRequest *req,
               remote::DeleteRangeResponse *resp,
               google::protobuf::Closure *done)
    {
        data_store_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name();
    }

    uint32_t GetPartitionId() const override
    {
        return req_->partition_id();
    }

    const std::string_view GetStartKey() const override
    {
        return req_->start_key();
    }

    const std::string_view GetEndKey() const override
    {
        return req_->end_key();
    }

    bool SkipWal() const override
    {
        return req_->skip_wal();
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);

        ::EloqDS::remote::CommonResult *res = resp_->mutable_result();
        res->set_error_code(result.error_code());
        res->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *data_store_service_{nullptr};
    const remote::DeleteRangeRequest *req_{nullptr};
    remote::DeleteRangeResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class DeleteRangeLocalRequest : public DeleteRangeRequest
{
public:
    DeleteRangeLocalRequest() = default;
    DeleteRangeLocalRequest(const DeleteRangeLocalRequest &other) = delete;
    DeleteRangeLocalRequest &operator=(const DeleteRangeLocalRequest &other) =
        delete;

    void Clear() override
    {
        table_name_ = "";
        partition_id_ = 0;
        start_key_ = "";
        end_key_ = "";
        skip_wal_ = false;
        result_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               const uint32_t partition_id,
               const std::string_view start_key,
               const std::string_view end_key,
               const bool skip_wal,
               remote::CommonResult &result,
               google::protobuf::Closure *done)
    {
        data_store_service_ = ds_service;
        table_name_ = table_name;
        partition_id_ = partition_id;
        start_key_ = start_key;
        end_key_ = end_key;
        skip_wal_ = skip_wal;
        result_ = &result;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    uint32_t GetPartitionId() const override
    {
        return partition_id_;
    }

    const std::string_view GetStartKey() const override
    {
        return start_key_;
    }

    const std::string_view GetEndKey() const override
    {
        return end_key_;
    }

    bool SkipWal() const override
    {
        return skip_wal_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        data_store_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        result_->set_error_code(result.error_code());
        result_->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *data_store_service_{nullptr};
    std::string_view table_name_{""};
    uint32_t partition_id_{0};
    std::string_view start_key_{""};
    std::string_view end_key_{""};
    bool skip_wal_{false};
    remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

/**
 * @brief Base class for all request objects.
 *        It provides the in_use flag to indicate whether the request object is
 *        in use.
 *        It should be only used in thread local pool since in_use flag is not
 * atomic.
 */
class ReadRequest : public Poolable
{
public:
    ReadRequest() = default;
    ReadRequest(const ReadRequest &other) = delete;
    ReadRequest &operator=(const ReadRequest &other) = delete;

    virtual ~ReadRequest() = default;

    // paramters in
    virtual const std::string_view GetTableName() const = 0;

    virtual const std::string_view GetKey() const = 0;

    virtual uint32_t GetPartitionId() const = 0;

    // parameters out
    virtual void SetRecord(std::string &&record) = 0;

    virtual void SetRecordTs(uint64_t record_ts) = 0;

    virtual void SetRecordTtl(uint64_t record_ttl) = 0;

    // finish
    virtual void SetFinish(
        const ::EloqDS::remote::DataStoreError error_code) = 0;
};

class ReadRpcRequest : public ReadRequest
{
public:
    ReadRpcRequest() = default;
    ReadRpcRequest(const ReadRequest &other) = delete;
    ReadRpcRequest &operator=(const ReadRequest &other) = delete;

    void Reset(DataStoreService *ds_service,
               const remote::ReadRequest *req,
               remote::ReadResponse *resp,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name();
    }

    const std::string_view GetKey() const override
    {
        return req_->key_str();
    }

    uint32_t GetPartitionId() const override
    {
        return req_->partition_id();
    }

    void SetRecord(std::string &&record) override
    {
        resp_->set_value(std::move(record));
    }

    void SetRecordTs(uint64_t record_ts) override
    {
        resp_->set_ts(record_ts);
    }

    void SetRecordTtl(uint64_t record_ttl) override
    {
        resp_->set_ttl(record_ttl);
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code) override
    {
        brpc::ClosureGuard done_guard(done_);
        if (error_code == remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_->PrepareShardingError(req_->partition_id(),
                                              resp_->mutable_result());
            return;
        }

        resp_->mutable_result()->set_error_code(error_code);
    }

private:
    DataStoreService *ds_service_{nullptr};
    const remote::ReadRequest *req_{nullptr};
    remote::ReadResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class ReadLocalRequest : public ReadRequest
{
public:
    ReadLocalRequest() = default;
    ReadLocalRequest(const ReadRequest &other) = delete;
    ReadLocalRequest &operator=(const ReadRequest &other) = delete;

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               const uint32_t partition_id,
               const std::string_view key,
               std::string *record,
               uint64_t *record_ts,
               uint64_t *record_ttl,
               ::EloqDS::remote::CommonResult *result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        table_name_ = table_name;
        key_ = key;
        partition_id_ = partition_id;
        record_ = record;
        record_ts_ = record_ts;
        record_ttl_ = record_ttl;
        result_ = result;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        table_name_ = "";
        key_ = "";
        partition_id_ = 0;
        record_ = nullptr;
        record_ts_ = nullptr;
        record_ttl_ = nullptr;
        result_ = nullptr;
        done_ = nullptr;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    const std::string_view GetKey() const override
    {
        return key_;
    }

    uint32_t GetPartitionId() const override
    {
        return partition_id_;
    }

    void SetRecord(std::string &&record) override
    {
        *record_ = std::move(record);
    }

    void SetRecordTs(uint64_t record_ts) override
    {
        *record_ts_ = record_ts;
    }

    void SetRecordTtl(uint64_t record_ttl) override
    {
        *record_ttl_ = record_ttl;
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code) override
    {
        brpc::ClosureGuard done_guard(done_);
        if (error_code == remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_->PrepareShardingError(partition_id_, result_);
            return;
        }

        result_->set_error_code(error_code);
    }

private:
    DataStoreService *ds_service_{nullptr};
    std::string_view table_name_{""};
    std::string_view key_{""};
    uint32_t partition_id_{0};
    std::string *record_{nullptr};
    uint64_t *record_ts_{nullptr};
    uint64_t *record_ttl_{nullptr};
    EloqDS::remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class CreateTableRequest : public Poolable
{
public:
    CreateTableRequest() = default;
    CreateTableRequest(const CreateTableRequest &other) = delete;
    CreateTableRequest &operator=(const CreateTableRequest &other) = delete;

    virtual ~CreateTableRequest() = default;

    // parameters in
    virtual const std::string_view GetTableName() const = 0;

    // finish
    virtual void SetFinish(const remote::CommonResult &result) = 0;
};

class CreateTableRpcRequest : public CreateTableRequest
{
public:
    CreateTableRpcRequest() = default;
    CreateTableRpcRequest(const CreateTableRpcRequest &other) = delete;
    CreateTableRpcRequest &operator=(const CreateTableRpcRequest &other) =
        delete;

    void Clear() override
    {
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const remote::CreateTableRequest *req,
               remote::CreateTableResponse *resp,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name();
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);

        ::EloqDS::remote::CommonResult *res = resp_->mutable_result();
        res->set_error_code(result.error_code());
        res->set_error_msg(result.error_msg());
    }

    const remote::CreateTableRequest *GetRequest()
    {
        return req_;
    }

    remote::CreateTableResponse *GetResponse()
    {
        return resp_;
    }

private:
    DataStoreService *ds_service_{nullptr};
    const remote::CreateTableRequest *req_{nullptr};
    remote::CreateTableResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class CreateTableLocalRequest : public CreateTableRequest
{
public:
    CreateTableLocalRequest() = default;
    CreateTableLocalRequest(const CreateTableLocalRequest &other) = delete;
    CreateTableLocalRequest &operator=(const CreateTableLocalRequest &other) =
        delete;

    void Clear() override
    {
        table_name_ = "";
        result_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               remote::CommonResult &result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        table_name_ = table_name;
        result_ = &result;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        result_->set_error_code(result.error_code());
        result_->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *ds_service_{nullptr};
    std::string_view table_name_{""};
    remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class DropTableRequest : public Poolable
{
public:
    DropTableRequest() = default;
    DropTableRequest(const DropTableRequest &other) = delete;
    DropTableRequest &operator=(const DropTableRequest &other) = delete;

    virtual ~DropTableRequest() = default;

    // parameters in
    virtual const std::string_view GetTableName() const = 0;

    // finish
    virtual void SetFinish(const remote::CommonResult &result) = 0;
};

class DropTableRpcRequest : public DropTableRequest
{
public:
    DropTableRpcRequest() = default;
    DropTableRpcRequest(const DropTableRpcRequest &other) = delete;
    DropTableRpcRequest &operator=(const DropTableRpcRequest &other) = delete;

    void Clear() override
    {
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const remote::DropTableRequest *req,
               remote::DropTableResponse *resp,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name();
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);

        ::EloqDS::remote::CommonResult *res = resp_->mutable_result();
        res->set_error_code(result.error_code());
        res->set_error_msg(result.error_msg());
    }

    const remote::DropTableRequest *GetRequest()
    {
        return req_;
    }

    remote::DropTableResponse *GetResponse()
    {
        return resp_;
    }

private:
    DataStoreService *ds_service_{nullptr};
    const remote::DropTableRequest *req_{nullptr};
    remote::DropTableResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class DropTableLocalRequest : public DropTableRequest
{
public:
    DropTableLocalRequest() = default;
    DropTableLocalRequest(const DropTableLocalRequest &other) = delete;
    DropTableLocalRequest &operator=(const DropTableLocalRequest &other) =
        delete;

    void Clear() override
    {
        table_name_ = "";
        result_ = nullptr;
        done_ = nullptr;
    }

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               remote::CommonResult &result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        table_name_ = table_name;
        result_ = &result;
        done_ = done;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    void SetFinish(const remote::CommonResult &result) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        result_->set_error_code(result.error_code());
        result_->set_error_msg(result.error_msg());
    }

private:
    DataStoreService *ds_service_{nullptr};
    std::string_view table_name_{""};
    remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class ScanRequest : public Poolable
{
public:
    ScanRequest() = default;
    ScanRequest(const ScanRequest &other) = delete;
    ScanRequest &operator=(const ScanRequest &other) = delete;

    virtual ~ScanRequest() = default;

    // parameters in
    virtual const std::string_view GetTableName() const = 0;

    virtual uint32_t GetPartitionId() const = 0;

    virtual const std::string_view GetStartKey() const = 0;

    virtual const std::string_view GetEndKey() const = 0;

    virtual bool InclusiveStart() const = 0;

    virtual bool InclusiveEnd() const = 0;

    virtual bool ScanForward() const = 0;

    virtual uint32_t BatchSize() const = 0;

    virtual int GetSearchConditionsSize() const = 0;

    virtual const remote::SearchCondition *GetSearchConditions(
        int index) const = 0;

    // parameters out
    virtual void AddItem(std::string &&key,
                         std::string &&value,
                         uint64_t ts,
                         uint64_t ttl) = 0;

    virtual void SetSessionId(const std::string &session_id) = 0;

    virtual void ClearSessionId() = 0;

    virtual const std::string &GetSessionId() = 0;

    // finish
    virtual void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                           const std::string error_message = "") = 0;
};

class ScanRpcRequest : public ScanRequest
{
public:
    ScanRpcRequest() = default;
    ScanRpcRequest(const ScanRpcRequest &other) = delete;
    ScanRpcRequest &operator=(const ScanRpcRequest &other) = delete;

    // Inner class to match the return type
    using SearchCondition = ::EloqDS::remote::SearchCondition;

    void Reset(DataStoreService *ds_service,
               const remote::ScanRequest *req,
               remote::ScanResponse *resp,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    const std::string_view GetTableName() const override
    {
        return req_->kv_table_name_str();
    }

    uint32_t GetPartitionId() const override
    {
        return req_->partition_id();
    }

    const std::string_view GetStartKey() const override
    {
        return req_->start_key();
    }

    const std::string_view GetEndKey() const override
    {
        return req_->end_key();
    }

    bool InclusiveStart() const override
    {
        return req_->inclusive_start();
    }

    bool InclusiveEnd() const override
    {
        return req_->inclusive_end();
    }

    bool ScanForward() const override
    {
        return req_->scan_forward();
    }

    uint32_t BatchSize() const override
    {
        return req_->batch_size();
    }

    int GetSearchConditionsSize() const override
    {
        return req_->search_conditions_size();
    }

    const remote::SearchCondition *GetSearchConditions(int index) const override
    {
        if (index >= req_->search_conditions_size())
        {
            return nullptr;
        }
        return &req_->search_conditions(index);
    }

    void AddItem(std::string &&key,
                 std::string &&value,
                 uint64_t ts,
                 uint64_t ttl) override
    {
        auto item = resp_->add_items();
        item->set_key(std::move(key));
        item->set_value(std::move(value));
        item->set_ts(ts);
        item->set_ttl(ttl);
    }

    void SetSessionId(const std::string &session_id) override
    {
        resp_->set_session_id(session_id);
    }

    void ClearSessionId() override
    {
        resp_->clear_session_id();
    }

    const std::string &GetSessionId() override
    {
        return req_->session_id();
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                   const std::string error_message) override
    {
        brpc::ClosureGuard done_guard(done_);
        if (error_code == remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_->PrepareShardingError(req_->partition_id(),
                                              resp_->mutable_result());
            return;
        }
        ::EloqDS::remote::CommonResult *result = resp_->mutable_result();
        result->set_error_code(error_code);
        result->set_error_msg(error_message);
    }

private:
    DataStoreService *ds_service_{nullptr};

    const remote::ScanRequest *req_{nullptr};
    remote::ScanResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
    std::vector<::EloqDS::remote::SearchCondition> search_conditions_;
};

class ScanLocalRequest : public ScanRequest
{
public:
    ScanLocalRequest() = default;
    ScanLocalRequest(const ScanLocalRequest &other) = delete;
    ScanLocalRequest &operator=(const ScanLocalRequest &other) = delete;

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               uint32_t partition_id,
               const std::string_view start_key,
               const std::string_view end_key,
               bool inclusive_start,
               bool inclusive_end,
               bool scan_forward,
               uint32_t batch_size,
               const std::vector<remote::SearchCondition> *search_conditions,
               std::vector<ScanTuple> *items,
               std::string *session_id,
               ::EloqDS::remote::CommonResult *result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        table_name_ = table_name;
        partition_id_ = partition_id;
        start_key_ = start_key;
        end_key_ = end_key;
        inclusive_start_ = inclusive_start;
        inclusive_end_ = inclusive_end;
        scan_forward_ = scan_forward;
        batch_size_ = batch_size;
        search_conditions_ = search_conditions;
        items_ = items;
        session_id_ = session_id;
        result_ = result;
        done_ = done;
    }

    void Reset(DataStoreService *ds_service,
               const std::string_view table_name,
               const uint32_t partition_id,
               std::string *session_id,
               ::EloqDS::remote::CommonResult *result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        table_name_ = table_name;
        partition_id_ = partition_id;
        session_id_ = session_id;
        result_ = result;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        table_name_ = "";
        partition_id_ = 0;
        start_key_ = "";
        end_key_ = "";
        inclusive_start_ = false;
        inclusive_end_ = false;
        scan_forward_ = false;
        batch_size_ = 0;
        search_conditions_ = nullptr;
        items_ = nullptr;
        session_id_ = nullptr;
        result_ = nullptr;
        done_ = nullptr;
    }

    const std::string_view GetTableName() const override
    {
        return table_name_;
    }

    uint32_t GetPartitionId() const override
    {
        return partition_id_;
    }

    const std::string_view GetStartKey() const override
    {
        return start_key_;
    }

    const std::string_view GetEndKey() const override
    {
        return end_key_;
    }

    bool InclusiveStart() const override
    {
        return inclusive_start_;
    }

    bool InclusiveEnd() const override
    {
        return inclusive_end_;
    }

    bool ScanForward() const override
    {
        return scan_forward_;
    }

    uint32_t BatchSize() const override
    {
        return batch_size_;
    }

    int GetSearchConditionsSize() const override
    {
        return search_conditions_ ? search_conditions_->size() : 0;
    }

    const remote::SearchCondition *GetSearchConditions(int index) const override
    {
        if (static_cast<size_t>(index) >= search_conditions_->size())
        {
            return nullptr;
        }
        return &search_conditions_->at(index);
    }

    void AddItem(std::string &&key,
                 std::string &&value,
                 uint64_t ts,
                 uint64_t ttl) override
    {
        items_->emplace_back(std::move(key), std::move(value), ts, ttl);
    }

    void SetSessionId(const std::string &session_id) override
    {
        *session_id_ = session_id;
    }

    void ClearSessionId() override
    {
        session_id_->clear();
    }

    const std::string &GetSessionId() override
    {
        return *session_id_;
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                   const std::string error_message) override
    {
        brpc::ClosureGuard done_guard(done_);
        if (error_code == remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_->PrepareShardingError(partition_id_, result_);
            return;
        }
        result_->set_error_code(error_code);
        result_->set_error_msg(error_message);
    }

private:
    DataStoreService *ds_service_{nullptr};
    std::string_view table_name_{""};
    uint32_t partition_id_{0};
    std::string_view start_key_{""};
    std::string_view end_key_{""};
    bool inclusive_start_{false};
    bool inclusive_end_{false};
    bool scan_forward_{false};
    uint32_t batch_size_{0};
    const std::vector<remote::SearchCondition> *search_conditions_{nullptr};
    std::vector<ScanTuple> *items_{nullptr};
    std::string *session_id_{nullptr};
    EloqDS::remote::CommonResult *result_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class CreateSnapshotForBackupRequest : public Poolable
{
public:
    CreateSnapshotForBackupRequest() = default;
    CreateSnapshotForBackupRequest(
        const CreateSnapshotForBackupRequest &other) = delete;
    CreateSnapshotForBackupRequest &operator=(
        const CreateSnapshotForBackupRequest &other) = delete;

    virtual ~CreateSnapshotForBackupRequest() = default;

    virtual std::string_view GetBackupName() const = 0;
    virtual uint64_t GetBackupTs() const = 0;
    virtual void AddBackupFile(const std::string &file) = 0;

    // finish
    virtual void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                           const std::string error_message = "") = 0;
};

class CreateSnapshotForBackupRpcRequest : public CreateSnapshotForBackupRequest
{
public:
    CreateSnapshotForBackupRpcRequest() = default;
    CreateSnapshotForBackupRpcRequest(
        const CreateSnapshotForBackupRpcRequest &other) = delete;
    CreateSnapshotForBackupRpcRequest &operator=(
        const CreateSnapshotForBackupRpcRequest &other) = delete;

    void Reset(DataStoreService *ds_service,
               const remote::CreateSnapshotForBackupRequest *req,
               remote::CreateSnapshotForBackupResponse *resp,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        req_ = req;
        resp_ = resp;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        req_ = nullptr;
        resp_ = nullptr;
        done_ = nullptr;
    }

    std::string_view GetBackupName() const override
    {
        return req_->backup_name();
    }

    uint64_t GetBackupTs() const override
    {
        return req_->backup_ts();
    }

    void AddBackupFile(const std::string &file) override
    {
        resp_->add_backup_files(file);
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                   const std::string error_message) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        ::EloqDS::remote::CommonResult *result = resp_->mutable_result();
        result->set_error_code(error_code);
        result->set_error_msg(error_message);
    }

private:
    DataStoreService *ds_service_{nullptr};
    const remote::CreateSnapshotForBackupRequest *req_{nullptr};
    remote::CreateSnapshotForBackupResponse *resp_{nullptr};
    google::protobuf::Closure *done_{nullptr};
};

class CreateSnapshotForBackupLocalRequest
    : public CreateSnapshotForBackupRequest
{
public:
    CreateSnapshotForBackupLocalRequest() = default;
    CreateSnapshotForBackupLocalRequest(
        const CreateSnapshotForBackupLocalRequest &other) = delete;
    CreateSnapshotForBackupLocalRequest &operator=(
        const CreateSnapshotForBackupLocalRequest &other) = delete;

    void Reset(DataStoreService *ds_service,
               std::string_view backup_name,
               const uint64_t backup_ts,
               std::vector<std::string> *backup_files,
               ::EloqDS::remote::CommonResult *result,
               google::protobuf::Closure *done)
    {
        ds_service_ = ds_service;
        backup_name_ = backup_name;
        backup_files_ = backup_files;
        backup_ts_ = backup_ts;
        result_ = result;
        done_ = done;
    }

    void Clear() override
    {
        ds_service_ = nullptr;
        backup_name_ = "";
        backup_files_ = nullptr;
        backup_ts_ = 0;
        result_ = nullptr;
        done_ = nullptr;
    }

    std::string_view GetBackupName() const override
    {
        return backup_name_;
    }

    void AddBackupFile(const std::string &file) override
    {
        backup_files_->emplace_back(file);
    }

    uint64_t GetBackupTs() const override
    {
        return backup_ts_;
    }

    void SetFinish(const ::EloqDS::remote::DataStoreError error_code,
                   const std::string error_message) override
    {
        ds_service_->DecreaseWriteReqCount();
        brpc::ClosureGuard done_guard(done_);
        result_->set_error_code(error_code);
        result_->set_error_msg(error_message);
    }

private:
    DataStoreService *ds_service_{nullptr};
    EloqDS::remote::CommonResult *result_{nullptr};
    std::string_view backup_name_{""};
    std::vector<std::string> *backup_files_{nullptr};
    uint64_t backup_ts_{0};
    google::protobuf::Closure *done_{nullptr};
};

}  // namespace EloqDS
