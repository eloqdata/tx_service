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
#include <memory>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
enum struct ContainerType
{
    // The container owns the data. It is a rvalue, so upon
    // assignment the ownership of the data is transfered.
    rvalue,
    // The container does not own the data and only keeps a reference (pointer).
    // The data outlives the container and is immutable throughout the tx
    // execution.
    immutable_ref,
    // The container keeps a reference of the data. But the data is mutable
    // throughout the tx execution, so if the tx machine needs to keep the data,
    // it needs to make a copy.
    mutable_ref
};

/// <summary>
/// This class is used to facilitate the management of ownership of tx keys to
/// reduce unnecessary memory allocation. A tx user creates tx keys/records and
/// pass them to the tx machine for reads/writes. The created keys/records are
/// of three types:
///
/// (1) they are rvalue and the tx user intends to transfer the
/// ownership to the tx machine, e.g., a SQL runtime creates a new key for each
/// read/write and never re-uses it once the read/write returns.
///
/// (2) the tx user owns the keys/records, outlive the tx machine and they are
/// immutable after the read/write, e.g., a stored procedure declares one key
/// for each read/write and the key is immutable throughout the procedure. The
/// procedure may run repeatedly, re-using the keys.
///
/// (3) the tx user owns the keys/records and the user may use them for other
/// purposes once each read/write returns.
/// </summary>
class TxKeyContainer
{
public:
    TxKeyContainer(std::unique_ptr<TxKey> data)
        : owner_(std::move(data)),
          ptr_(owner_.get()),
          type_(ContainerType::rvalue)
    {
    }

    TxKeyContainer(const TxKey *ptr,
                   ContainerType type = ContainerType::mutable_ref)
        : owner_(nullptr), ptr_(ptr), type_(type)
    {
    }

    TxKeyContainer(TxKeyContainer &&other) noexcept
        : owner_(std::move(other.owner_)), ptr_(other.ptr_), type_(other.type_)
    {
        other.type_ = ContainerType::immutable_ref;
    }

    TxKeyContainer &operator=(TxKeyContainer &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (owner_ != nullptr)
        {
            *owner_ = *rhs.ptr_;
            return *this;
        }

        switch (rhs.type_)
        {
        case txservice::ContainerType::rvalue:
        {
            // The assignment moves the data ownership to the assignee and makes
            // the original container a reference to the new owner.
            owner_ = std::move(rhs.owner_);
            ptr_ = owner_.get();
            type_ = ContainerType::rvalue;

            rhs.type_ = ContainerType::immutable_ref;
            rhs.ptr_ = ptr_;
            break;
        }
        case txservice::ContainerType::immutable_ref:
        {
            // The assignment makes a copy of the reference.
            ptr_ = rhs.ptr_;
            type_ = ContainerType::immutable_ref;
            break;
        }
        case txservice::ContainerType::mutable_ref:
        {
            // The assignment makes a clone of the data, because the original
            // container may change after the assignment.
            owner_ = rhs.ptr_->Clone();
            ptr_ = owner_.get();
            type_ = ContainerType::rvalue;
            break;
        }
        default:
            break;
        }

        return *this;
    }

    friend bool operator==(const TxKeyContainer &lhs, const TxKeyContainer &rhs)
    {
        return *lhs.get() == *rhs.get();
    }

    const TxKey *get() const
    {
        return ptr_;
    }

private:
    std::unique_ptr<TxKey> owner_;
    const TxKey *ptr_;
    ContainerType type_;
};

/// <summary>
/// Similar to TxKeyContainer
/// </summary>
class TxRecordContainer
{
public:
    TxRecordContainer(std::unique_ptr<TxRecord> data)
        : owner_(std::move(data)),
          ptr_(owner_.get()),
          type_(ContainerType::rvalue)
    {
    }

    TxRecordContainer(TxRecord *ptr,
                      ContainerType type = ContainerType::mutable_ref)
        : owner_(nullptr),
          ptr_(ptr),
          type_(ptr == nullptr ? ContainerType::rvalue : type)
    {
    }

    TxRecordContainer(TxRecordContainer &&other) noexcept
        : owner_(std::move(other.owner_)), ptr_(other.ptr_), type_(other.type_)
    {
        other.type_ = ContainerType::immutable_ref;
    }

    TxRecordContainer &operator=(TxRecordContainer &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        // value-copy with two rvalues not working
        // if (owner_ != nullptr)
        // {
        //     *owner_ = *rhs.ptr_;
        //     return *this;
        // }

        switch (rhs.type_)
        {
        case txservice::ContainerType::rvalue:
            // The assignment moves the data ownership to the assignee and makes
            // the original container a reference to the new owner.
            owner_ = std::move(rhs.owner_);
            ptr_ = owner_.get();
            type_ = ContainerType::rvalue;

            rhs.type_ = ContainerType::immutable_ref;
            rhs.ptr_ = ptr_;
            break;
        case txservice::ContainerType::immutable_ref:
            // The assignment makes a copy of the immutable reference.
            owner_ = nullptr;
            ptr_ = rhs.ptr_;
            type_ = ContainerType::immutable_ref;
            break;
        case txservice::ContainerType::mutable_ref:
            // The assignment makes a clone of the data, because the original
            // container may change after the assignment.
            owner_ = rhs.ptr_->Clone();
            ptr_ = owner_.get();
            type_ = ContainerType::rvalue;
            break;
        default:
            break;
        }

        return *this;
    }

    void Set(TxRecord *data)
    {
        if (type_ == ContainerType::immutable_ref)
        {
            assert(ptr_ != nullptr);
            if (data != nullptr)
            {
                *ptr_ = *data;
            }
        }
        else if (data != nullptr)
        {
            owner_ = data->Clone();
            ptr_ = owner_.get();
            type_ = ContainerType::rvalue;
        }
        else
        {
            owner_ = nullptr;
            ptr_ = nullptr;
            type_ = ContainerType::rvalue;
        }
    }

    TxRecord *get() const
    {
        return ptr_;
    }

    /*TxRecord::Uptr GetRecord()
    {
        if (type_ == ContainerType::rvalue)
        {
            return std::move(owner_);
        }
        else
        {
            return ptr_->Clone();
        }
    }*/

private:
    std::unique_ptr<TxRecord> owner_;
    TxRecord *ptr_;
    ContainerType type_;
};
}  // namespace txservice
