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

#include <algorithm>
#include <deque>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "tx_id.h"
#include "tx_key.h"
#include "tx_object.h"
#include "tx_record.h"

namespace txservice
{
struct TxObject;
struct LruEntry;
class CcShard;

struct TxCommandResult
{
public:
    virtual ~TxCommandResult() = default;
    virtual void Serialize(std::string &buf) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
};

enum class ExecResult
{
    Fail,   // Failed to execute command
    Read,   // Success to execute readonly command
    Write,  // Succes to execute the command and modified object.
    Block,  // The command is blocked
    Unlock  // There has not expected result and release ccentry lock
};

enum class BlockOperation
{
    NoBlock,     // Not block operation type
    PopBlock,    // Pop an element if has or block until expired or insert an
                 // element
    PopNoBlock,  // Pop an element if has or return empty.
    BlockLock,   // BLock on the object until the object has at least one
                 // element, then lock the obj and return
    PopElement,  // Pop the element, only used after BlockLock.
    Discard      // To discard the blocked command
};

struct TxCommand
{
public:
    virtual ~TxCommand() = default;
    virtual std::unique_ptr<TxCommand> Clone() = 0;
    virtual bool IsReadOnly() const = 0;
    // If this value overwrites old value.
    virtual bool IsOverwrite() const
    {
        return false;
    }
    // Only true for DEL.
    virtual bool IsDelete() const
    {
        return false;
    }
    // If this commands does not need previous object value. Note that
    // this is different with IsOverwrite since some of the commands overwrites
    // old value but need to return the status / value of the old object.
    virtual bool IgnoreKvValue() const
    {
        return false;
    }
    virtual std::unique_ptr<TxRecord> CreateObject(
        const std::string *image) const = 0;

    // Get command for retiring command (e.g. DelCommand) for expired TTL
    // bound object
    virtual std::unique_ptr<TxCommand> RetireExpiredTTLObjectCommand() const
    {
        assert(false);
        return nullptr;
    }

    // This command for restoring ttl object from wal
    virtual std::unique_ptr<TxCommand> RecoverTTLObjectCommand()
    {
        assert(false);
        return nullptr;
    }

    // Will this command change the object ttl, currently used for purpose if
    // the object ttl will be reseted or not
    virtual bool WillSetTTL() const
    {
        return false;
    }

    virtual std::unique_ptr<TxCommandResult> CreateCommandResult() const = 0;
    virtual bool ProceedOnNonExistentObject() const = 0;
    virtual bool ProceedOnExistentObject() const = 0;
    /**
     * Execute cmd on object to get the result.
     * @param object
     * @return return ExecResult, ObjectCcMap.Execute will has different
     * response with different return value.
     */
    virtual ExecResult ExecuteOn(const TxObject &object) = 0;

    // Commit current command on obj_ptr, return the new object if the command
    // changes or deletes the object. Read only command need not commit.
    virtual TxObject *CommitOn(TxObject *obj_ptr)
    {
        assert(false);
        return obj_ptr;
    }

    // serialize command for remote request and writing log
    virtual void Serialize(std::string &str) const
    {
        assert(false);
    }

    // deserialize a command from binary blob for processing remote request and
    // replaying log
    virtual void Deserialize(std::string_view cmd_img)
    {
        assert(false);
    }

    virtual TxCommandResult *GetResult() = 0;

    // To Judge if this command passed to execute, or failed
    // The default result is true;
    // If a transaction need to execute more than one command, and one of them
    // failed, it need to set all command that has executed abort. So it need
    // RedisServiceImpl::SimpleCommand and other methods call this methods to
    // know if this command passed or failed, then decide if the transaction is
    // continue or abort.
    virtual bool IsPassed() const
    {
        // TODO(lzx): replace "ObjectCommandResult::cmd_success_" with this.
        return true;
    }
    // If this command will be existing until the transaction committed.
    // True: It will be destroy after execute and need to clone for commit.
    // False: It will always exist until commited.
    virtual bool IsVolatile() = 0;
    // The default value is not need to clone, for lua, it should call this
    // method to set Volatile to true
    virtual void SetVolatile() = 0;

    // Pop a blocked request from the queue if exist. The reason to add this
    // method is due to it maybe needs some conditions with the related object.
    // for example the object should has elements.
    virtual bool AblePopBlockRequest(TxObject *object) const
    {
        return false;
    }

    virtual BlockOperation GetBlockOperationType()
    {
        return BlockOperation::NoBlock;
    }
};

/**
 * Commands that operate on multiple keys, like MSET, DEL.
 */
struct MultiObjectTxCommand
{
    virtual ~MultiObjectTxCommand() = default;

    virtual std::vector<TxKey> *KeyPointers() = 0;

    virtual std::vector<TxCommand *> *CommandPointers() = 0;

    // For block commands, it need to rewrite below 4 methods to support
    // flexible steps.
    virtual bool IsFinished() = 0;
    virtual bool IsLastStep() = 0;
    virtual size_t CmdSteps() = 0;

    virtual void IncrSteps() = 0;
    // If it has two parts of commands and finished to run the first part, it
    // should call below method to collect the result and fill the second part
    // of commands, then run the second part.
    //@return true: Need to run the second part of command; false: Not need to
    // run
    virtual bool HandleMiddleResult()
    {
        assert(false);
        return false;
    }

    // To judge if all commands passed. If at least one command failed, return
    // false, or return true. If is_two_parts_=true, it will according to
    // is_second_time_ to judge the first part or the second part.
    // The default return is true
    virtual bool IsPassed() const
    {
        return true;
    }

    // To judge if this block command is expired or not.
    virtual bool IsExpired() const
    {
        return false;
    }
    // The number of finished block commands. For block commands, they are not
    // need to wait all block commands to finished, one or some of them are
    // finished, the results can be satisfied, and it need the surplus commands
    // to abort. If return 0. means it is not block command.
    // Not all steps have blocked commands, maybe only one step has, other steps
    // should return 0
    virtual uint32_t NumOfFinishBlockCommands() const
    {
        return 0;
    }
    // Only called when NumOfFinishBlockCommands()>0 and (expired or the related
    // commands have finished). In this method, it will decide which child
    // commands should be discard and which is the next step to run.
    // @return true: need to discard obsolete cc request and go to next step;
    //          false: Only go to the next step and wait all local cc request
    //          to finish
    virtual bool ForwardResult()
    {
        assert(false);
        return false;
    }

    virtual bool IsBlockCommand()
    {
        return false;
    }
};

// commands and information of the same txn
struct TxnCmd
{
    TxnCmd(uint64_t obj_ver,
           uint64_t commit_ts,
           bool ignore_previous_version,
           std::vector<std::unique_ptr<TxCommand>> &&cmd_list)
        : obj_version_(obj_ver),
          new_version_(commit_ts),
          ignore_previous_version_(ignore_previous_version),
          cmd_list_(std::move(cmd_list))
    {
    }

    friend std::ostream &operator<<(std::ostream &os, const TxnCmd &txn)
    {
        os << "TxnCmd object version: " << txn.obj_version_
           << ", new version: " << txn.new_version_
           << ", ignore previous version: " << txn.ignore_previous_version_
           << "\n commands:";

        for (const auto &cmd : txn.cmd_list_)
        {
            os << typeid(*cmd).name() << ", ";
        }

        return os;
    }

    // the commit_ts of the object when commands of this txn applies to
    // it
    uint64_t obj_version_{};
    // commit_ts of the txn
    uint64_t new_version_{};
    // whether this txn can ignore the previous version of this object
    bool ignore_previous_version_{};
    // the commands the txn applies to this object
    std::vector<std::unique_ptr<TxCommand>> cmd_list_;
};

struct BufferedTxnCmdList
{
    std::deque<TxnCmd> txn_cmd_list_;

    bool Empty() const
    {
        return txn_cmd_list_.empty();
    }

    void Clear()
    {
        txn_cmd_list_.clear();
        txn_cmd_list_.shrink_to_fit();
    }

    size_t Size() const
    {
        return txn_cmd_list_.size();
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    const BufferedTxnCmdList &txn_cmd_list)
    {
        os << "BufferedTxnCmdList size: " << txn_cmd_list.Size()
           << ", detailed TxnCmds: ";
        for (const auto &txn_cmd : txn_cmd_list.txn_cmd_list_)
        {
            os << txn_cmd << " && ";
        }

        return os;
    }

    void EmplaceTxnCmd(TxnCmd &txn_cmd)
    {
        auto cmp = [](const TxnCmd &lhs, const TxnCmd &rhs) -> bool
        { return lhs.new_version_ < rhs.new_version_; };

        std::deque<TxnCmd> &txn_cmd_list = txn_cmd_list_;

        auto lb_it = std::lower_bound(
            txn_cmd_list.begin(), txn_cmd_list.end(), txn_cmd, cmp);
        if (lb_it != txn_cmd_list.end() &&
            lb_it->new_version_ == txn_cmd.new_version_)
        {
            if (lb_it->obj_version_ != txn_cmd.obj_version_)
            {
                LOG(ERROR)
                    << "Two TxnCmds with the same commit ts have different "
                       "object old version, should never happen.\nCurrent "
                       "TxnCmd: "
                    << txn_cmd << "\n"
                    << *this;
                assert(false);
            }
            // same txn cmd already exists, discard duplicate cmd
            DLOG(INFO) << "TxnCmd: " << txn_cmd
                       << " emplace into command list again, skip";
            return;
        }

        if (txn_cmd.ignore_previous_version_)
        {
            // For commands that overwrite objects or commit on deleted objects,
            // remove the txn commands before this txn.
            lb_it = txn_cmd_list.erase(txn_cmd_list.begin(), lb_it);
        }
        txn_cmd_list.insert(lb_it, std::move(txn_cmd));
    }
};

/**
 * Commit replay txn commands in version order and update cur_ver.
 *
 * @tparam T
 * @param payload
 * @param buffered_cmd_list
 * @param cur_ver
 */
template <class T>
void TryCommitBufferedCommands(std::unique_ptr<T> &payload,
                               BufferedTxnCmdList &buffered_cmd_list,
                               uint64_t &cur_ver)
{
    std::deque<TxnCmd> &txn_cmd_list = buffered_cmd_list.txn_cmd_list_;
    // iterate the list and apply the commands in version order
    auto it = txn_cmd_list.begin();
    while (it != txn_cmd_list.end())
    {
        if (it->ignore_previous_version_)
        {
            // If a TxnCmd ignores previous version, the TxnCmds before it
            // must have been discarded in EmplaceTxnCmd.
            assert(it == txn_cmd_list.begin());

            assert(it->obj_version_ >= cur_ver || it->obj_version_ == 1);
            cur_ver = it->obj_version_;
        }

        // check version match
        if (it->obj_version_ != cur_ver)
        {
            break;
        }

        if (!it->cmd_list_.empty())
        {
            auto &first_cmd = it->cmd_list_.front();

            // If a command was applied on deleted record, we set
            // `has_overwrite` flag to true in the log. If the first command
            // doesn't have an overwrite property, we need to create an
            // empty object.
            if (it->ignore_previous_version_ && !first_cmd->IsOverwrite())
            {
                payload.reset(nullptr);
            }
        }

        // apply the commands of this txn
        for (auto &cmd : it->cmd_list_)
        {
            if (payload == nullptr)
            {
                std::unique_ptr<TxRecord> obj_ptr = cmd->CreateObject(nullptr);
                payload.reset(reinterpret_cast<T *>(obj_ptr.release()));
            }
            TxObject *obj_ptr = reinterpret_cast<TxObject *>(payload.get());
            TxObject *new_obj_ptr =
                reinterpret_cast<TxObject *>(cmd->CommitOn(obj_ptr));
            if (new_obj_ptr != obj_ptr)
            {
                TxObject *old_payload_obj =
                    reinterpret_cast<TxObject *>(payload.release());
                // FIXME(lzx): should we use "new_obj_ptr->Clone()" ?
                payload.reset(reinterpret_cast<T *>(new_obj_ptr));
                if (old_payload_obj != nullptr)
                {
                    // Explicitly delete the old object to prevent memory leak
                    delete old_payload_obj;
                }
            }
        }
        cur_ver = it->new_version_;
        ++it;
    }
    txn_cmd_list.erase(txn_cmd_list.begin(), it);

    if (txn_cmd_list.empty())
    {
        DLOG(INFO) << "destruct buffered_cmd_list_ on object";
        buffered_cmd_list.Clear();
    }
    else
    {
        DLOG(INFO) << "replay not finished, current ver: " << cur_ver
                   << ", msg expect ver: " << txn_cmd_list.front().obj_version_
                   << ", msg commit ts: " << txn_cmd_list.front().new_version_;
    }
}

/**
 * The replayed commands must apply in order. Replayed commands are first
 * stored in buffered_cmd_list_ and committed in order.
 * @param obj_ver
 * @param commit_ts
 * @param cmd_list
 * @param cur_ver
 */
template <class T>
void EmplaceAndCommitBufferedTxnCommand(std::unique_ptr<T> &payload,
                                        BufferedTxnCmdList &buffered_cmd_list,
                                        TxnCmd &txn_cmd,
                                        uint64_t &cur_ver,
                                        RecordStatus &status)
{
    bool waiting_for_fetch = status == RecordStatus::Unknown;
    bool try_commit = txn_cmd.ignore_previous_version_ || !waiting_for_fetch;

    buffered_cmd_list.EmplaceTxnCmd(txn_cmd);

    if (try_commit)
    {
        TryCommitBufferedCommands(payload, buffered_cmd_list, cur_ver);
        status =
            payload == nullptr ? RecordStatus::Deleted : RecordStatus::Normal;
    }
}

}  // namespace txservice
