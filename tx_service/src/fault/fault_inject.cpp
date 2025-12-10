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
#include "fault/fault_inject.h"

#include "cc/cc_handler_result.h"
#include "proto/cc_request.pb.h"
#include "remote/cc_stream_sender.h"
#include "sharder.h"
#include "tx_request.h"

namespace txservice
{
using namespace remote;
void FaultInject::TriggerAction(FaultEntry *entry)
{
    if (entry->start_strike_ >= 0 && entry->end_strike_ >= 0)
    {
        entry->count_strike_++;
        if (entry->start_strike_ < entry->count_strike_ ||
            entry->end_strike_ > entry->count_strike_)
        {
            return;
        }
    }

    for (auto str : entry->vctAction_)
    {
        std::string action, para;
        size_t pos = str.find('<');
        if (pos == std::string::npos)
        {
            pos = str.find('#');
            if (pos == std::string::npos)
            {
                action = str;
            }
            else
            {
                action = str.substr(0, pos);
                para = str.substr(pos + 1, str.size() - pos - 1);
            }
        }
        else
        {
            action = str.substr(0, pos);
            size_t pos2 = str.find('>');
            if (pos2 == std::string::npos)
            {
                LOG(ERROR) << "Error action parameters: name="
                           << entry->fault_name_ << ", action=" << str;
                abort();
            }

            para = str.substr(pos + 1, pos2 - pos - 1);
        }

        std::transform(action.begin(), action.end(), action.begin(), ::toupper);
        auto iter = action_name_to_enum_map.find(action);
        FaultAction fa =
            (iter == action_name_to_enum_map.end() ? FaultAction::NOOP
                                                   : iter->second);

        switch (fa)
        {
        case FaultAction::PANIC:
        {
            int retval;
            sigset_t new_mask;
            sigfillset(&new_mask);

            retval = kill(getpid(), SIGKILL);
            assert(retval == 0);
            retval = sigsuspend(&new_mask);
            fprintf(
                stderr, "sigsuspend returned %d errno %d \n", retval, errno);
            assert(false); /* With full signal mask, we should never return
                              here. */
            break;
        }
        case FaultAction::SLEEP:
        {
            int secs = 1;
            if (!para.empty())
            {
                secs = std::stoi(para);
            }

            sleep(secs);
            break;
        }
        case FaultAction::REMOTE:
        {
            size_t pos1 = 0;
            size_t pos2 = para.find(';');
            if (pos2 == std::string::npos)
                pos2 = para.size();
            std::string fault_name = para.substr(0, pos2);

            std::string fault_paras;
            std::vector<int> vctId;
            pos1 = pos2 + 1;

            while (pos1 < para.size())
            {
                pos2 = para.find(';', pos1);
                if (pos2 == std::string::npos)
                    pos2 = para.size();

                std::string sbs = para.substr(pos1, pos2 - pos1);
                size_t pos3 = sbs.find('=');
                if (pos3 == std::string::npos)
                {
                    LOG(ERROR) << "Error action parameters: name="
                               << entry->fault_name_ << ", action=" << str;
                    abort();
                }

                std::string key = sbs.substr(0, pos3);
                std::string val = sbs.substr(pos3 + 1);

                if (key.compare("node_id") == 0)
                {
                    size_t nos1 = 0;
                    while (nos1 < val.size())
                    {
                        size_t nos2 = val.find('#', nos1);
                        if (nos2 == std::string::npos)
                            nos2 = val.size();

                        int id = stoi(val.substr(nos1, nos2 - nos1));
                        if (id >= (int) Sharder::Instance().NodeGroupCount())
                        {
                            LOG(ERROR)
                                << "Error remote node id: name="
                                << entry->fault_name_ << ", action=" << str;
                            abort();
                        }

                        vctId.push_back(id);
                        nos1 = nos2 + 1;
                    }
                }
                else
                {
                    if (fault_paras.size() > 0)
                        fault_paras += ";";
                    fault_paras += key + "=" + val;
                }

                pos1 = pos2 + 1;
            }

            if (vctId.size() == 0)
                break;

            for (int id : vctId)
            {
                uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(id);

                CcHandlerResult<bool> hres(nullptr);
                remote::CcStreamSender *ss = nullptr;
                remote::CcMessage send_msg;
                ss = Sharder::Instance().GetCcStreamSender();
                bool b = (ss != nullptr);

                // Try to send message to remote node first. If fail, send to
                // local node as default
                if (b)
                {
                    send_msg.set_type(
                        remote::CcMessage::MessageType::
                            CcMessage_MessageType_FaultInjectRequest);
                    send_msg.set_handler_addr(
                        reinterpret_cast<uint64_t>(&hres));
                    send_msg.set_tx_term(0);
                    send_msg.set_tx_number(0);

                    remote::FaultInjectRequest *fi_req =
                        send_msg.mutable_fault_inject_req();
                    fi_req->set_src_node_id(0);
                    fi_req->set_fault_name(fault_name);
                    fi_req->set_fault_paras(fault_paras);

                    auto send_res = ss->SendMessageToNode(dest_node_id, send_msg);
                    b = (send_res.sent || send_res.queued_for_retry);
                }

                // If CcStreamSender == nullptr or failed to send, send to local
                // node.
                if (!b)
                {
                    InjectFault(fault_name, fault_paras);
                }
            }
            break;
        }
        case FaultAction::LOG_TRANSFER:
        {
            size_t pos = para.find('-');
            if (pos == std::string::npos)
            {
                LOG(ERROR)
                    << "Error LOG_TRANSFER parameters: The right style should "
                       "be: action=LOG_TRANSFER#[group id]-[node id]";
                abort();
            }

            uint32_t log_group_id = std::stoul(para.substr(0, pos));
            uint32_t leader_idx = std::stoul(para.substr(pos + 1));
            Sharder::Instance().LogTransferLeader(log_group_id, leader_idx);
            break;
        }
        case FaultAction::CLEAN_PKMAP:
        {
            Sharder::Instance().CleanCcTable(TableName(
                para, TableType::Primary, txservice::TableEngine::EloqSql));
            break;
        }
        case FaultAction::NOTIFY_CHECKPOINTER:
        {
            Sharder::Instance().NotifyCheckPointer();
        }
        default:
            break;
        }
    }
}

}  // namespace txservice
