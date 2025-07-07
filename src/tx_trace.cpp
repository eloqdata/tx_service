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
#include "tx_trace.h"

#include <ostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_protocol.h"
#include "cc_request.h"
#include "cc_request.pb.h"
#include "remote/cc_stream_receiver.h"
#include "remote/cc_stream_sender.h"
#include "remote_cc_request.h"
#include "tx_operation.h"
#include "tx_request.h"
#include "type.h"

namespace txservice
{
#ifndef TX_TRACE_DISABLED
static const char *associate_fmt =
    "{\"associate\":{\"%s\":\"%x\",\"%s\":\"%x\",\"id\":\"%s\"}}";
static const char *associate_fmt_context =
    "{\"associate\":{\"%s\":\"%x\",\"%s\":\"%x\",\"id\":\"%s\", %s}}";

static const char *action_fmt =
    "{\"action\":{\"%s\":\"%x\",\"action\":\"%s\",\"%s\":\"%x\"}}";
static const char *action_fmt_context =
    "{\"action\":{\"%s\":\"%x\",\"action\":\"%s\",\"%s\":\"%x\",%s}}";

static const char *dump_fmt = "{\"dump\":{\"%s\":\"%x\",\"dump\":\"%s\"}}";
static const char *dump_fmt_tx_number =
    "{\"dump\":{\"%s\":\"%d\",\"dump\":\"%s\"}}";
static const char *dump_fmt_context =
    "{\"dump\":{\"%s\":\"%x\",\"dump\":\"%s\",%s}}";
static const char *dump_fmt_context_tx_number =
    "{\"dump\":{\"%s\":\"%d\",\"dump\":\"%s\",%s}}";

template <typename... Args>
std::string fmt(const char *fmt, Args... args)
{
    int sz = std::snprintf(nullptr, 0, fmt, args...) + 1;  //+1 for '\0'
    assert(sz > 0);
    size_t buf_sz = static_cast<size_t>(sz);
    auto buf = std::make_unique<char[]>(buf_sz);
    std::snprintf(buf.get(), buf_sz, fmt, args...);
    return std::string(buf.get(),
                       buf.get() + buf_sz - 1);  //-1 for removing '\0'
};

std::string fmt_hex(uint64_t i)
{
    return fmt("%x", i);
};

template <typename T>
std::string demangled_type_name(T t)
{
    const char *c_mangled_type_name = typeid(t).name();
    int status = 0;
    char *c_demangled_type_name =
        abi::__cxa_demangle(c_mangled_type_name, nullptr, nullptr, &status);
    std::string type_name;
    if (c_demangled_type_name)
    {
        type_name = c_demangled_type_name;
        free(c_demangled_type_name);
    }
    else
    {
        type_name = c_mangled_type_name;
    }
    return type_name;
};

/**
 * tx_trace_associate
 */
template <typename T, typename K>
std::string tx_trace_associate(T *t,
                               K *k,
                               std::string id,
                               std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::string context_string = context_func();
        return fmt(txservice::associate_fmt_context,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   demangled_type_name(k).c_str(),
                   reinterpret_cast<uint64_t>(k),
                   id.c_str(),
                   context_string.c_str());
    }
    else
    {
        return fmt(txservice::associate_fmt,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   demangled_type_name(k).c_str(),
                   reinterpret_cast<uint64_t>(k),
                   id.c_str());
    }
};
// Op and Result
template std::string tx_trace_associate(
    txservice::InitTxnOperation *,
    txservice::CcHandlerResult<txservice::InitTxResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::ReadOperation *,
    txservice::CcHandlerResult<ReadKeyResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::AcquireWriteOperation *,
    txservice::CcHandlerResult<std::vector<AcquireKeyResult>> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::SetCommitTsOperation *,
    txservice::CcHandlerResult<uint64_t> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::ValidateOperation *,
    txservice::CcHandlerResult<PostProcessResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::WriteToLogOp *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::UpdateTxnStatus *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::PostProcessOp *,
    txservice::CcHandlerResult<PostProcessResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::PostProcessOp *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::FaultInjectOp *,
    txservice::CcHandlerResult<bool> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::CleanCcEntryForTestOp *,
    txservice::CcHandlerResult<bool> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::ScanOpenOperation *,
    txservice::CcHandlerResult<ScanOpenResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::ScanNextOperation *,
    txservice::CcHandlerResult<ScanNextResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::AcquireAllOp *,
    txservice::CcHandlerResult<AcquireAllResult> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::PostWriteAllOp *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::DsUpsertTableOp *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::NoOp *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::DsOp<Void> *,
    txservice::CcHandlerResult<Void> *,
    std::string,
    std::function<std::string()> context_func);

// Op and Op
template std::string tx_trace_associate(
    txservice::UpsertTableOp *,
    txservice::AcquireAllOp *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::UpsertTableOp *,
    txservice::WriteToLogOp *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::UpsertTableOp *,
    txservice::PostWriteAllOp *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::UpsertTableOp *,
    txservice::DsUpsertTableOp *,
    std::string,
    std::function<std::string()> context_func);

// tx and handler
template std::string tx_trace_associate(
    txservice::TransactionExecution *,
    txservice::CcHandler *,
    std::string,
    std::function<std::string()> context_func);
// CcMessage and RemoteCcRequest
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteAcquire *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteAcquireAll *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemotePostRead *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteRead *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteReadOutside *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemotePostWrite *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemotePostWriteAll *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteScanOpen *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteScanNextBatch *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteFaultInjectCC *,
    std::string,
    std::function<std::string()> context_func);
template std::string tx_trace_associate(
    txservice::remote::CcMessage *,
    txservice::remote::RemoteCleanCcEntryForTestCc *,
    std::string,
    std::function<std::string()> context_func);

// CcMap and CcEntry
template std::string tx_trace_associate(
    txservice::CcMap *,
    txservice::LruEntry *,
    std::string,
    std::function<std::string()> context_func);
/**
 * tx_trace_action
 */
template <typename T, typename K>
std::string tx_trace_action(T *t,
                            std::string action,
                            K k,
                            std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::string context_string = context_func();
        return fmt(txservice::action_fmt_context,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   action.c_str(),
                   demangled_type_name(k).c_str(),
                   k,
                   context_string.c_str());
    }
    else
    {
        return fmt(txservice::action_fmt,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   action.c_str(),
                   demangled_type_name(k).c_str(),
                   k);
    }
};
template <typename T, typename K>
std::string tx_trace_action(T *t,
                            std::string action,
                            K *k,
                            std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::string context_string = context_func();
        return fmt(txservice::action_fmt_context,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   action.c_str(),
                   demangled_type_name(k).c_str(),
                   reinterpret_cast<uint64_t>(k),
                   context_string.c_str());
    }
    else
    {
        return fmt(txservice::action_fmt,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   action.c_str(),
                   demangled_type_name(k).c_str(),
                   reinterpret_cast<uint64_t>(k));
    }
};
// Tx and TxRequest
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::InitTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ReadTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ReadOutsideTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ScanOpenTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ScanBatchTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ScanCloseTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::UpsertTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::CommitTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::AbortTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::UpsertTableTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::FaultInjectTxRequest *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::CleanCcEntryForTestTxRequest *,
                                     std::function<std::string()>);

// Tx and Op
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::InitTxnOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ReadOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ScanOpenOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ScanNextOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::AcquireWriteOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::SetCommitTsOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ValidateOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::UpsertTableOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::PostProcessOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::WriteToLogOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::SleepOperation *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::FaultInjectOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::CleanCcEntryForTestOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::TxKey *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::AcquireAllOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::PostWriteAllOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::DsUpsertTableOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::UpdateTxnStatus *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::NoOp *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::DsOp<RangeMedianKeyResult> *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::DsOp<Void> *,
                                     std::function<std::string()>);

template std::string tx_trace_action(txservice::TransactionExecution *,
                                     std::string,
                                     txservice::ReadWriteSet *,
                                     std::function<std::string()>);
// Op and Tx
template std::string tx_trace_action(txservice::InitTxnOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ReadOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ScanOpenOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ScanNextOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::AcquireWriteOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::SetCommitTsOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ValidateOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::UpsertTableOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::PostProcessOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::WriteToLogOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::SleepOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::FaultInjectOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CleanCcEntryForTestOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TxKey *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::AcquireAllOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::PostWriteAllOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::DsUpsertTableOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::UpdateTxnStatus *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::TransactionOperation *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::NoOp *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::DsOp<RangeMedianKeyResult> *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::DsOp<Void> *,
                                     std::string,
                                     txservice::TransactionExecution *,
                                     std::function<std::string()>);
// CcMessage
template std::string tx_trace_action(txservice::remote::CcStreamSender *,
                                     std::string,
                                     txservice::remote::CcMessage *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::remote::CcStreamReceiver *,
                                     std::string,
                                     txservice::remote::CcMessage *,
                                     std::function<std::string()>);

// CcHandlerResult and TxOperation Result
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::InitTxResult> *,
    std::string,
    txservice::InitTxResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::InitTxResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ReadKeyResult> *,
    std::string,
    txservice::ReadKeyResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ReadKeyResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ScanNextResult> *,
    std::string,
    txservice::ScanNextResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ScanNextResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ScanOpenResult> *,
    std::string,
    txservice::ScanOpenResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::ScanOpenResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::AcquireAllResult> *,
    std::string,
    txservice::AcquireAllResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::AcquireAllResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::AcquireKeyResult> *,
    std::string,
    txservice::AcquireKeyResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<std::vector<AcquireKeyResult>> *,
    std::string,
    std::vector<AcquireKeyResult> *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::PostProcessResult> *,
    std::string,
    txservice::PostProcessResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::AcquireKeyResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<std::vector<txservice::AcquireKeyResult>> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::PostProcessResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::Void> *,
    std::string,
    txservice::Void *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::Void> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<std::vector<txservice::TxId>> *,
    std::string,
    std::vector<txservice::TxId> *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<std::vector<txservice::TxId>> *,
    std::string,
    int8_t,
    std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<TxId> *,
                                     std::string,
                                     txservice::TxId *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<TxId> *,
                                     std::string,
                                     int8_t,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<bool> *,
                                     std::string,
                                     bool *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<bool> *,
                                     std::string,
                                     int8_t,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<uint64_t> *,
                                     std::string,
                                     uint64_t *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<uint64_t> *,
                                     std::string,
                                     int8_t,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<int8_t> *,
                                     std::string,
                                     int8_t *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcHandlerResult<int8_t> *,
                                     std::string,
                                     int8_t,
                                     std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::RangeScanSliceResult> *,
    std::string,
    txservice::RangeScanSliceResult *,
    std::function<std::string()>);
template std::string tx_trace_action(
    txservice::CcHandlerResult<txservice::RangeScanSliceResult> *,
    std::string,
    int8_t,
    std::function<std::string()>);

template std::string tx_trace_action(txservice::remote::CcStreamSender *,
                                     std::string,
                                     const txservice::remote::CcMessage *,
                                     std::function<std::string()>);
// CcRequest
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::AcquireCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::AcquireAllCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::PostWriteAllCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::PostWriteCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::PostReadCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::ReadCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::ScanOpenBatchCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::ScanNextBatchCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::NegotiateCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::FaultInjectCC *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::LocalCcHandler *,
                                     std::string,
                                     txservice::CleanCcEntryForTestCc *,
                                     std::function<std::string()>);

template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::AcquireCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::AcquireAllCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::PostWriteAllCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::PostWriteCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::PostReadCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::ReadCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::ScanOpenBatchCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::ScanNextBatchCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::ReplayLogCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::NegotiateCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::FaultInjectCC *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::CleanCcEntryForTestCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::CkptScanCc *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::remote::RemoteScanOpen *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::remote::RemoteReadOutside *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::remote::RemoteScanNextBatch *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::NonBlockingLock *,
                                     std::function<std::string()>);
// CcRequest and LruEntry
template std::string tx_trace_action(txservice::CcRequestBase *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::AcquireCc *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::AcquireAllCc *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ScanNextBatchCc *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ScanOpenBatchCc *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::ReadCc *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::remote::RemoteScanOpen *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::remote::RemoteScanNextBatch *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
// CcMap and cce

template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::LruEntry *,
                                     std::function<std::string()>);
template std::string tx_trace_action(txservice::CcMap *,
                                     std::string,
                                     txservice::CcRequestBase *,
                                     std::function<std::string()>);

/**
 * tx_trace_dump
 */
std::ostream &operator<<(std::ostream &outs, txservice::TxId *r)
{
    outs << "{tx_number_:" << r->TxNumber()
         << ",global_core_id_:" << r->GlobalCoreId()
         << ",ident_:" << r->Identity() << ",vec_idx_:" << r->VecIdx() << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::InitTxResult *r)
{
    outs << "{txid_:" << &r->txid_ << ",start_ts_:" << r->start_ts_
         << ",term_:" << r->term_ << "}";
    return outs;
};
template <typename K>
std::ostream &operator<<(std::ostream &outs, std::vector<K> *r)
{
    outs << "[";
    for (typename std::vector<K>::const_iterator iter = r->begin();
         iter != r->end();
         ++iter)
    {
        if (iter != r->begin())
        {
            outs << ",";
        }
        outs << &iter;
    }

    outs << "]";
    return outs;
};
template std::ostream &operator<<(std::ostream &outs, std::vector<int64_t> *r);
template std::ostream &operator<<(std::ostream &outs,
                                  std::vector<txservice::TxId> *r);
template <typename K>
std::ostream &operator<<(std::ostream &outs, std::unordered_set<K> *r)
{
    outs << "[";
    for (typename std::unordered_set<K>::const_iterator iter = r->begin();
         iter != r->end();
         ++iter)
    {
        if (iter != r->begin())
        {
            outs << ",";
        }
        outs << &iter;
    }

    outs << "]";
    return outs;
};
template std::ostream &operator<<(std::ostream &outs,
                                  std::unordered_set<uint64_t> *r);

std::ostream &operator<<(std::ostream &outs,
                         const std::unordered_set<uint64_t> *r)
{
    outs << "[";
    for (typename std::unordered_set<TxNumber>::const_iterator iter =
             r->begin();
         iter != r->end();
         ++iter)
    {
        if (iter != r->begin())
        {
            outs << ",";
        }
        outs << *iter;
    }

    outs << "]";
    return outs;
};

std::ostream &operator<<(std::ostream &outs, txservice::RecordStatus *r)
{
    std::string status;
    switch (*r)
    {
    case txservice::RecordStatus::Normal:
        status = "Normal";
        break;
    case txservice::RecordStatus::Unknown:
        status = "Unknown";
        break;
    case txservice::RecordStatus::Deleted:
        status = "Deleted";
        break;
    case txservice::RecordStatus::RemoteUnknown:
        status = "RemoteUnknown";
        break;
    default:
        status = "[Unknown Status]";
    };

    outs << status;
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CcEntryAddr *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{cce_ptr_:" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",insert_ptr_:" << r->InsertPtr()
         << ",node_group_id_:" << r->NodeGroupId() << ",term_:" << r->Term()
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, const txservice::CcEntryAddr *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{cce_ptr_:" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",insert_ptr_:" << r->InsertPtr()
         << ",node_group_id_:" << r->NodeGroupId() << ",term_:" << r->Term()
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ReadKeyResult *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{cce_addr_:" << &r->cce_addr_ << ",ts_:" << r->ts_
         << ",rec_status_:" << &r->rec_status_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CcScanner *r)
{
    if (!r)
    {
        return outs << "[]";
    }
    outs << "[";
    std::vector<std::pair<uint32_t, size_t>> shard_code_and_sizes;
    r->ShardCacheSizes(&shard_code_and_sizes);
    for (std::vector<std::pair<uint32_t, size_t>>::iterator iter =
             shard_code_and_sizes.begin();
         iter != shard_code_and_sizes.end();
         ++iter)
    {
        if (iter != shard_code_and_sizes.begin())
        {
            outs << ",";
        }
        std::pair<uint32_t, size_t> shard_code_and_size = *iter;
        uint32_t shard_code = shard_code_and_size.first;
        size_t scan_cache_size = shard_code_and_size.second;
        outs << "{shard_code:" << shard_code
             << ",scan_cache_size:" << scan_cache_size << "}";
    }
    outs << "]";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ScanOpenResult *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{scanner_:" << r->scanner_.get()
         << ",scan_alias_:" << r->scan_alias_
         << ",cc_node_terms_:" << &r->cc_node_terms_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ScanNextResult *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{term_:" << r->term_ << ",node_group_id_:" << r->node_group_id_
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::AcquireAllResult *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{last_vali_ts_:" << r->last_vali_ts_
         << ",commit_ts_:" << r->commit_ts_ << ",node_term_:" << r->node_term_
         << ",local_cce_addr_:" << &r->local_cce_addr_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::AcquireKeyResult *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{last_vali_ts_:" << r->last_vali_ts_
         << ",commit_ts_:" << r->commit_ts_ << ",cce_addr_:" << &r->cce_addr_
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, bool *r)
{
    if (!r)
    {
        return outs << false;
    }
    outs << *r;
    return outs;
};
std::ostream &operator<<(std::ostream &outs, uint64_t *r)
{
    if (!r)
    {
        return outs << 0;
    }
    outs << *r;
    return outs;
};
std::string cc_message_type_to_string(
    txservice::remote::CcMessage::MessageType type)
{
    std::string msg_type;
    switch (type)
    {
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_Shutdown:
        msg_type = "CcMessage_MessageType_Shutdown";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_AcquireRequest:
        msg_type = "CcMessage_MessageType_AcquireRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_AcquireResponse:
        msg_type = "CcMessage_MessageType_AcquireRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ReadRequest:
        msg_type = "CcMessage_MessageType_ReadRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ReadResponse:
        msg_type = "CcMessage_MessageType_ReadResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ValidateRequest:
        msg_type = "CcMessage_MessageType_ValidateRequest ";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ValidateResponse:
        msg_type = "CcMessage_MessageType_ValidateResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_PostprocessResponse:
        msg_type = "CcMessage_MessageType_PostprocessResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_PostCommitRequest:
        msg_type = "CcMessage_MessageType_PostCommitRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ScanOpenRequest:
        msg_type = "CcMessage_MessageType_ScanOpenRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ScanOpenResponse:
        msg_type = "CcMessage_MessageType_ScanOpenResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ScanNextRequest:
        msg_type = "CcMessage_MessageType_ScanNextRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ScanNextResponse:
        msg_type = "CcMessage_MessageType_ScanNextResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_ReadOutsideRequest:
        msg_type = "CcMessage_MessageType_ReadOutsideRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_FaultInjectRequest:
        msg_type = "CcMessage_MessageType_FaultInjectRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_FaultInjectResponse:
        msg_type = "CcMessage_MessageType_FaultInjectResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestRequest:
        msg_type = "CcMessage_MessageType_CleanCcEntryForTestRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestResponse:
        msg_type = "CcMessage_MessageType_CleanCcEntryForTestResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_AcquireAllRequest:
        msg_type = "CcMessage_MessageType_AcquireAllRequest";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_AcquireAllResponse:
        msg_type = "CcMessage_MessageType_AcquireAllResponse";
        break;
    case txservice::remote::CcMessage::MessageType::
        CcMessage_MessageType_PostWriteAllRequest:
        msg_type = "CcMessage_MessageType_PostWriteAllRequest";
        break;
    default:
        msg_type = "[Unknown CcMessage Type]";
    };

    return msg_type;
};
std::ostream &operator<<(std::ostream &outs, txservice::remote::CcMessage *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    std::string msg_type = cc_message_type_to_string(r->type());
    outs << "{\"type\":\"" << msg_type << "\",\"handler_addr\":\""
         << fmt_hex(r->handler_addr()) << ",\"tx_number\":\"" << r->tx_number()
         << "\""
         << ",\"tx_term\":\"" << r->tx_term() << "\"}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         const txservice::remote::CcMessage *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    std::string msg_type = cc_message_type_to_string(r->type());
    outs << "{\"type\":\"" << msg_type << "\",\"handler_addr\":\""
         << fmt_hex(r->handler_addr()) << ",\"tx_number\":\"" << r->tx_number()
         << "\""
         << ",\"tx_term\":\"" << r->tx_term() << "\"}";
    return outs;
};
// CcRequest ostream
std::ostream &operator<<(std::ostream &outs, txservice::IsolationLevel r)
{
    std::string iso_level;
    switch (r)
    {
    case txservice::IsolationLevel::ReadCommitted:
        iso_level = "ReadCommited";
        break;
    case txservice::IsolationLevel::RepeatableRead:
        iso_level = "RepeatableRead";
        break;
    case txservice::IsolationLevel::Serializable:
        iso_level = "Serializable";
        break;
    case txservice::IsolationLevel::Snapshot:
        iso_level = "Snapshot";
        break;
    default:
        iso_level = "[Unknown Isolation Level]";
    };
    outs << iso_level;
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CcProtocol r)
{
    std::string proto;
    switch (r)
    {
    case txservice::CcProtocol::Locking:
        proto = "Locking";
        break;
    case txservice::CcProtocol::OCC:
        proto = "OCC";
        break;
    case txservice::CcProtocol::OccRead:
        proto = "OccRead";
        break;
    default:
        proto = "[Unknown Cc Protocal]";
    };
    outs << proto;
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::LockType r)
{
    std::string lock_type;
    switch (r)
    {
    case txservice::LockType::NoLock:
        lock_type = "NoLock";
        break;
    case txservice::LockType::ReadLock:
        lock_type = "ReadLock";
        break;
    case txservice::LockType::ReadIntent:
        lock_type = "ReadIntent";
        break;
    case txservice::LockType::WriteLock:
        lock_type = "WriteLock";
        break;
    case txservice::LockType::WriteIntent:
        lock_type = "WriteIntent";
        break;
    default:
        lock_type = "[Unknown Lock Type]";
    };
    outs << lock_type;
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CcOperation r)
{
    std::string cc_op;
    switch (r)
    {
    case txservice::CcOperation::Read:
        cc_op = "Read";
        break;
    case txservice::CcOperation::ReadForWrite:
        cc_op = "ReadForWrite";
        break;
    case txservice::CcOperation::ReadSkIndex:
        cc_op = "ReadSkIndex";
        break;
    case txservice::CcOperation::Write:
        cc_op = "Write";
        break;
    default:
        cc_op = "[Unknown Cc Operation]";
    };
    outs << cc_op;
    return outs;
};

std::ostream &operator<<(std::ostream &outs, txservice::OperationType r)
{
    std::string dml_op;
    switch (r)
    {
    case txservice::OperationType::Update:
        dml_op = "Update";
        break;
    case txservice::OperationType::Delete:
        dml_op = "Delete";
        break;
    case txservice::OperationType::Insert:
        dml_op = "Insert";
        break;
    case txservice::OperationType::Upsert:
        dml_op = "Upsert";
        break;
    case txservice::OperationType::CreateTable:
        dml_op = "CreateTable";
        break;
    case txservice::OperationType::DropTable:
        dml_op = "DropTable";
        break;
    case txservice::OperationType::AddIndex:
        dml_op = "AddIndex";
        break;
    case txservice::OperationType::DropIndex:
        dml_op = "DropIndex";
        break;
    default:
        dml_op = "[Unknown DML and DDL OperationType]";
    };
    outs << dml_op;
    return outs;
}

std::ostream &operator<<(std::ostream &outs, txservice::PostWriteType r)
{
    std::string pwt;
    switch (r)
    {
    case txservice::PostWriteType::Commit:
        pwt = "Commit";
        break;
    case txservice::PostWriteType::PrepareCommit:
        pwt = "PrepareCommit";
        break;
    case txservice::PostWriteType::PostCommit:
        pwt = "PostCommit";
        break;
    default:
        pwt = "[Unknown Post Write Type]";
    };
    outs << pwt;
    return outs;
}
std::ostream &operator<<(std::ostream &outs, txservice::ReadType r)
{
    std::string read_type;
    switch (r)
    {
    case txservice::ReadType::Inside:
        read_type = "Inside";
        break;
    case txservice::ReadType::OutsideDeleted:
        read_type = "OutsideDeleted";
        break;
    case txservice::ReadType::OutsideNormal:
        read_type = "OutsideNormal";
        break;
    default:
        read_type = "[Unknown Read Type]";
    }
    outs << read_type;
    return outs;
}
std::ostream &operator<<(std::ostream &outs, txservice::ScanIndexType r)
{
    std::string idx_t;
    switch (r)
    {
    case txservice::ScanIndexType::Primary:
        idx_t = "Primary";
        break;
    case txservice::ScanIndexType::Secondary:
        idx_t = "Secondary";
        break;
    default:
        idx_t = "[Unknown Scan Index Type]";
    }
    outs << idx_t;
    return outs;
}
std::ostream &operator<<(std::ostream &outs, txservice::ScanDirection r)
{
    std::string d;
    switch (r)
    {
    case txservice::ScanDirection::Forward:
        d = "Forward";
        break;
    case txservice::ScanDirection::Backward:
        d = "Backward";
        break;
    default:
        d = "[Unknown Scan Direction]";
    }
    outs << d;
    return outs;
}
std::ostream &operator<<(std::ostream &outs, txservice::TxnStatus r)
{
    std::string s;
    switch (r)
    {
    case txservice::TxnStatus::Aborted:
        s = "Abort";
        break;
    case txservice::TxnStatus::Committed:
        s = "Committed";
        break;
    case txservice::TxnStatus::Committing:
        s = "Commiting";
        break;
    case txservice::TxnStatus::Finished:
        s = "Finished";
        break;
    case txservice::TxnStatus::Ongoing:
        s = "Ongoing";
        break;
    case txservice::TxnStatus::Recovering:
        s = "Recovering";
        break;
    default:
        s = "[Unknown Tx Status]";
    }
    outs << s;
    return outs;
}
std::ostream &operator<<(std::ostream &outs, txservice::AcquireCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"key_shard_code\":" << r->KeyShardCode()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm() << ",\"ts_\":" << r->Ts()
         << ",\"is_insert_\":" << r->IsInsert()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteAcquire *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"key_shard_code\":" << r->KeyShardCode()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm() << ",\"ts_\":" << r->Ts()
         << ",\"is_insert_\":" << r->IsInsert()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::AcquireAllCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm()
         << ",\"is_insert_\":" << r->IsInsert()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",\"cc_op_\":\"" << r->CcOp() << "\""
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteAcquireAll *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm()
         << ",\"is_insert_\":" << r->IsInsert()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",\"cc_op_\":\"" << r->CcOp() << "\""
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::PostWriteCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"cce_ptr_\":" << r->CceAddr()
         << ",\"commit_ts_\":" << r->CommitTs()
         << ",\"payload_\":" << FMT_POINTER_TO_UINT64T(r->Payload())
         << ",\"payload_str_\":" << FMT_POINTER_TO_UINT64T(r->PayloadStr())
         << ",\"operation_type_\":" << r->GetOperationType() << "}"
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemotePostWrite *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"cce_ptr_\":" << r->CceAddr()
         << ",\"commit_ts_\":" << r->CommitTs()
         << ",\"payload_\":" << FMT_POINTER_TO_UINT64T(r->Payload())
         << ",\"payload_str_\":" << FMT_POINTER_TO_UINT64T(r->PayloadStr())
         << ",\"operation_type_\":" << r->GetOperationType()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::PostWriteAllCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"commit_ts_\":" << r->CommitTs()
         << ",\"payload_\":" << FMT_POINTER_TO_UINT64T(r->Payload())
         << ",\"payload_str_\":" << FMT_POINTER_TO_UINT64T(r->PayloadStr())
         << ",\"op_type_\":\"" << r->OpType()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result()) << "\""
         << ",\"commit_type_\":\"" << r->CommitType() << "\""
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemotePostWriteAll *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << ",\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyStr())
         << ",\"commit_ts_\":" << r->CommitTs()
         << ",\"payload_\":" << FMT_POINTER_TO_UINT64T(r->Payload())
         << ",\"payload_str_\":" << FMT_POINTER_TO_UINT64T(r->PayloadStr())
         << ",\"op_type_\":\"" << r->OpType()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result()) << "\""
         << ",\"commit_type_\":\"" << r->CommitType() << "\""
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::PostReadCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << ",\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"cce_ptr_\":" << r->CceAddr()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"commit_ts_\":" << r->CommitTs() << ",\"key_ts_\":" << r->KeyTs()
         << ",\"gap_ts_\":" << r->GapTs() << ",\"lock_type_\":\""
         << r->GetLockType() << "\""
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemotePostRead *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"cce_ptr_\":" << r->CceAddr()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"commit_ts_\":" << r->CommitTs() << ",\"key_ts_\":" << r->KeyTs()
         << ",\"gap_ts_\":" << r->GapTs() << ",\"lock_type_\":\""
         << r->GetLockType() << "\""
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ReadCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r)
         << "\""
         //<< ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_\":"
         << (r->Key() != nullptr ? r->Key()->ToString() : "")
         //<< ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyBlob())
         << ",\"key_str_\":" << (r->KeyBlob() != nullptr ? *r->KeyBlob() : "")
         << ",\"key_shard_code\":" << r->KeyShardCode()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm() << ",\"ts_\":" << r->ReadTimestamp()
         << ",\"type_\":\"" << r->Type() << "\""
         << ",\"is_for_write_\":\"" << r->IsForWrite() << "\""
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::remote::RemoteRead *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r)
         << "\""
         //<< ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key())
         << ",\"key_\":"
         << (r->Key() != nullptr ? r->Key()->ToString() : "")
         //<< ",\"key_str_\":" << FMT_POINTER_TO_UINT64T(r->KeyBlob())
         << ",\"key_str_\":" << (r->KeyBlob() != nullptr ? *r->KeyBlob() : "")
         << ",\"key_shard_code\":" << r->KeyShardCode()
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term\":" << r->TxTerm() << ",\"ts_\":" << r->ReadTimestamp()
         << ",\"type_\":\"" << r->Type() << "\""
         << ",\"is_for_write_\":\"" << r->IsForWrite() << "\""
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteReadOutside *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"cce_ptr_\":" << &r->CceAddr()
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ScanOpenBatchCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"index_type_\":\"" << r->index_type_
         << "\""
         //<< ",\"start_key_\":" << FMT_POINTER_TO_UINT64T(r->start_key_)
         << ",\"start_key_\":"
         << (r->start_key_ != nullptr ? r->start_key_->ToString() : "")
         << ",\"inclusive_\":" << r->inclusive_ << ",\"direct_\":\""
         << r->direct_ << "\""
         << ",\"ts_\":" << r->ts_
         << ",\"scan_cache_\":" << FMT_POINTER_TO_UINT64T(r->scan_cache_)
         << ",\"term_\":" << r->term_ << ",\"is_for_write_\":\""
         << r->IsForWrite() << "\""
         << ",\"is_ckpt_delta_\":" << r->is_ckpt_delta_
         << ",\"is_include_floor_cce_\":" << r->is_include_floor_cce_
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteScanOpen *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"tx_term\":" << r->TxTerm()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr(0))
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ScanNextBatchCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"ts_\":" << r->ts_
         << ",\"scan_cache_\":" << FMT_POINTER_TO_UINT64T(r->scan_cache_)
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"tx_term_\":" << r->tx_term_
         << ",\"is_ckpt_delta_\":" << r->is_ckpt_delta_
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteScanNextBatch *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"tx_term\":" << r->TxTerm()
         << ",\"cce_ptr_\":" << FMT_POINTER_TO_UINT64T(r->CcePtr())
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ScanCloseCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"alias_\":" << r->alias_
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->key_)
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->Result())
         << ",\"inclusive_\":" << r->inclusive_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CkptTsCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"ckpt_ts_\":" << r->GetCkptTs() << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CkptScanCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << "\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_\":\"" << r->table_name_.StringView() << "\""
         << ",\"ckpt_ts_\":" << r->ckpt_ts_
         << ",\"node_group_\":" << r->node_group_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::NegotiateCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->txid_->TxNumber()
         << ",\"isolation_level\":\"" << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"tx_ts_\":" << r->tx_ts_
         << ",\"res_\":" << FMT_POINTER_TO_UINT64T(r->res_) << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::CheckTxStatusCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << ",\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"tx_status_\":" << r->tx_status_ << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ClearTxCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << ",\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::ReplayLogCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"tx_number_\":" << r->Txn() << ",\"isolation_level\":\""
         << r->Isolation() << "\""
         << ",\"proto_\":\"" << r->Protocol() << "\""
         << ",\"table_name_holder_\":\"" << r->table_name_holder_.StringView()
         << "\""
         << "}";
    return outs;
};
std::ostream &operator<<(std::ostream &outs, txservice::FaultInjectCC *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"fault_name_\":\"" << r->FaultName() << "\""
         << ",\"fault_paras_\":\"" << r->FaultParas() << "\""
         << "}";
    return outs;
}
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteFaultInjectCC *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"fault_name_\":\"" << r->FaultName() << "\""
         << ",\"fault_paras_\":\"" << r->FaultParas() << "\""
         << ",\"handler_addr\":" << fmt_hex(r->handler_addr()) << "}";
    return outs;
}
std::ostream &operator<<(std::ostream &outs,
                         txservice::CleanCcEntryForTestCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }

    outs << "{\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key()) << "}";
    return outs;
}
std::ostream &operator<<(std::ostream &outs,
                         txservice::remote::RemoteCleanCcEntryForTestCc *r)
{
    if (!r)
    {
        return outs << "{}";
    }
    outs << "{\"table_name_\":\"" << GET_TABLE_NAME(r) << "\""
         << ",\"key_\":" << FMT_POINTER_TO_UINT64T(r->Key()) << "}";
    return outs;
}
// template tx_trace_dump
template <typename T>
std::string tx_trace_dump(T *t, std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::ostringstream oss;
        oss << t;
        std::string context_string = context_func();
        return fmt(txservice::dump_fmt_context,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   oss.str().c_str(),
                   context_string.c_str());
    }
    else
    {
        std::ostringstream oss;
        oss << t;
        return fmt(txservice::dump_fmt,
                   demangled_type_name(t).c_str(),
                   reinterpret_cast<uint64_t>(t),
                   oss.str().c_str());
    }
};
// TxOperation Result
template std::string tx_trace_dump(txservice::InitTxResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ReadKeyResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ScanOpenResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ScanNextResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::AcquireAllResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::AcquireKeyResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(std::vector<txservice::AcquireKeyResult> *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::PostProcessResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::GenerateSkParallelResult *,
                                   std::function<std::string()>);
// CcRequest
template std::string tx_trace_dump(txservice::AcquireCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::AcquireAllCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::PostWriteAllCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::PostWriteCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::PostReadCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ReadCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ScanOpenBatchCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ScanNextBatchCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::NegotiateCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::FaultInjectCC *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::CleanCcEntryForTestCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::CkptScanCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ReplayLogCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::ScanSliceCc *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteAcquire *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteAcquireAll *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemotePostRead *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteRead *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteReadOutside *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemotePostWrite *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemotePostWriteAll *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteScanOpen *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteScanNextBatch *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::RemoteFaultInjectCC *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(
    txservice::remote::RemoteCleanCcEntryForTestCc *,
    std::function<std::string()>);
// Other dump
template std::string tx_trace_dump(txservice::TxId *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(std::vector<txservice::TxId> *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(const std::unordered_set<uint64_t> *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(std::unordered_set<uint64_t> *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(bool *, std::function<std::string()>);
template std::string tx_trace_dump(uint64_t *, std::function<std::string()>);
template std::string tx_trace_dump(int8_t *, std::function<std::string()>);
template std::string tx_trace_dump(const txservice::remote::CcMessage *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::remote::CcMessage *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::RangeMedianKeyResult *,
                                   std::function<std::string()>);
template std::string tx_trace_dump(txservice::RangeScanSliceResult *,
                                   std::function<std::string()>);
// TxOp
std::string tx_trace_dump(txservice::Void *result,
                          std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::string context_string = context_func();
        return fmt(txservice::dump_fmt_context,
                   "txservice::Void*",
                   reinterpret_cast<uint64_t>(result),
                   "",
                   context_string.c_str());
    }
    else
    {
        return fmt(txservice::dump_fmt,
                   "txservice::Void*",
                   reinterpret_cast<uint64_t>(result),
                   "");
    }
};
std::string tx_trace_dump(txservice::TxNumber tx_number,
                          std::function<std::string()> context_func)
{
    if (context_func)
    {
        std::string context_string = context_func();
        return fmt(txservice::dump_fmt_context_tx_number,
                   "txservice::tx_number",
                   tx_number,
                   "",
                   context_string.c_str());
    }
    else
    {
        return fmt(txservice::dump_fmt_tx_number,
                   "txservice::tx_number",
                   tx_number,
                   "");
    }
};

#endif
}  // namespace txservice
