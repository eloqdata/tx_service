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

#include <mimalloc.h>

#include <cassert>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "schema.h"
#include "tx_serialize.h"

namespace txservice
{
enum class KeyType : uint8_t
{
    NegativeInf = 0,
    PositiveInf,
    Normal
};

class TxKey;

class TxKeyInterface
{
public:
    TxKeyInterface() = delete;

    template <typename T>
    TxKeyInterface(const T &)
        : delete_func_(
              [](void *obj_ptr)
              {
                  T *key = static_cast<T *>(obj_ptr);
                  delete key;
              }),
          equal_func_(
              [](const void *this_obj, const void *rhs_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  const T *rhs_key = static_cast<const T *>(rhs_obj);
                  return *key == *rhs_key;
              }),
          less_func_(
              [](const void *this_obj, const void *rhs_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  const T *rhs_key = static_cast<const T *>(rhs_obj);
                  return *key < *rhs_key;
              }),
          hash_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->Hash();
              }),
          serialize_str_func_(
              [](const void *this_obj, std::string &str)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  key->Serialize(str);
              }),
          kv_serialize_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->KVSerialize();
              }),
          serialize_len_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->SerializedLength();
              }),
          clone_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->CloneTxKey();
              }),
          copy_func_(
              [](void *this_obj, const void *input_obj)
              {
                  T *key = static_cast<T *>(this_obj);
                  const T *input = static_cast<const T *>(input_obj);
                  key->Copy(*input);
              }),
          mem_usage_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->MemUsage();
              }),
          set_packed_key_func_(
              [](void *this_obj, const char *data, size_t size)
              {
                  T *key = static_cast<T *>(this_obj);
                  return key->SetPackedKey(data, size);
              }),
          data_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->Data();
              }),
          size_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->Size();
              }),
          type_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->Type();
              }),
          needs_defrag_func_(
              [](void *this_obj, mi_heap_t *heap)
              {
                  T *key = static_cast<T *>(this_obj);
                  return key->NeedsDefrag(heap);
              }),
          to_string_func_(
              [](const void *this_obj)
              {
                  const T *key = static_cast<const T *>(this_obj);
                  return key->ToString();
              })
    {
    }

    void Delete(void *obj_ptr) const
    {
        assert(delete_func_ != nullptr);
        assert(obj_ptr != nullptr);
        delete_func_(obj_ptr);
    }

    bool Equal(const void *this_obj, const void *rhs_obj) const
    {
        return equal_func_(this_obj, rhs_obj);
    }

    bool LessThan(const void *this_obj, const void *rhs_obj) const
    {
        return less_func_(this_obj, rhs_obj);
    }

    size_t Hash(const void *this_obj) const
    {
        return hash_func_(this_obj);
    }

    void SerializeStr(const void *this_obj, std::string &str) const
    {
        serialize_str_func_(this_obj, str);
    }

    std::string_view KVSerialize(const void *this_obj) const
    {
        return kv_serialize_func_(this_obj);
    }

    size_t SerializedLen(const void *this_obj) const
    {
        return serialize_len_func_(this_obj);
    }

    TxKey Clone(const void *this_obj) const;

    void Copy(void *this_obj, const void *input_obj) const
    {
        copy_func_(this_obj, input_obj);
    }

    size_t MemUsage(const void *this_obj) const
    {
        return mem_usage_func_(this_obj);
    }

    void SetPackedKey(void *this_obj, const char *data, size_t size) const
    {
        set_packed_key_func_(this_obj, data, size);
    }

    const char *Data(const void *this_obj) const
    {
        return data_func_(this_obj);
    }

    size_t Size(const void *this_obj) const
    {
        return size_func_(this_obj);
    }

    KeyType Type(const void *this_obj) const
    {
        return this_obj != nullptr ? type_func_(this_obj)
                                   : KeyType::NegativeInf;
    }

    bool NeedsDefrag(void *this_obj, mi_heap_t *heap) const
    {
        return needs_defrag_func_(this_obj, heap);
    }

    std::string ToString(const void *this_obj) const
    {
        return this_obj != nullptr ? to_string_func_(this_obj)
                                   : std::string("NULL");
    }

protected:
    typedef void (*DeleteFunc)(void *obj_ptr);
    DeleteFunc const delete_func_;

    typedef bool (*EqualFunc)(const void *this_obj, const void *rhs_obj);
    EqualFunc const equal_func_;

    typedef bool (*LessThanFunc)(const void *this_obj, const void *rhs_obj);
    LessThanFunc const less_func_;

    typedef size_t (*HashFunc)(const void *this_obj);
    HashFunc const hash_func_;

    typedef void (*SerializeStrFunc)(const void *this_obj, std::string &str);
    SerializeStrFunc const serialize_str_func_;

    typedef std::string_view (*KvSerializeFunc)(const void *this_obj);
    KvSerializeFunc const kv_serialize_func_;

    typedef size_t (*SerializeLenFunc)(const void *this_obj);
    SerializeLenFunc const serialize_len_func_;

    typedef TxKey (*CloneFunc)(const void *this_obj);
    CloneFunc const clone_func_;

    typedef void (*CopyFunc)(void *this_obj, const void *input_obj);
    CopyFunc const copy_func_;

    typedef size_t (*MemUsageFunc)(const void *this_obj);
    MemUsageFunc const mem_usage_func_;

    typedef void (*SetPackedKeyFunc)(void *this_obj,
                                     const char *data,
                                     size_t size);
    SetPackedKeyFunc const set_packed_key_func_;

    typedef const char *(*DataFunc)(const void *this_obj);
    DataFunc const data_func_;

    typedef size_t (*SizeFunc)(const void *this_obj);
    SizeFunc const size_func_;

    typedef KeyType (*TypeFunc)(const void *this_obj);
    TypeFunc const type_func_;

    typedef bool (*NeedsDefragFunc)(void *this_obj, mi_heap_t *heap);
    NeedsDefragFunc const needs_defrag_func_;

    typedef std::string (*ToStringFunc)(const void *this_obj);
    ToStringFunc const to_string_func_;
};

class TxKey
{
public:
    // The default constructor constructs a tx key whose key type is negative
    // infinity.
    explicit TxKey() = default;

    template <typename T>
    explicit TxKey(T *key) : interface_(T::TxKeyImpl())
    {
        obj_addr_ = reinterpret_cast<uint64_t>(key);
    }

    template <typename T>
    explicit TxKey(const T *key) : interface_(T::TxKeyImpl())
    {
        obj_addr_ = reinterpret_cast<uint64_t>(key);
    }

    template <typename T>
    explicit TxKey(std::unique_ptr<T> key) : interface_(T::TxKeyImpl())
    {
        obj_addr_ = reinterpret_cast<uint64_t>(key.release());
        // Memory addresses are even numbers. We use the lowest bit to denote
        // ownership.
        obj_addr_ = obj_addr_ | 1;
    }

    TxKey(TxKey &&rhs) noexcept
        : interface_(rhs.interface_), obj_addr_(rhs.obj_addr_)
    {
        // The ownership, if there is any, is transferred to this object. So,
        // the input's ownership bit is 0 thereafter.
        rhs.obj_addr_ = rhs.obj_addr_ & mask;
    }

    ~TxKey()
    {
        if (IsOwner())
        {
            void *obj_ptr = GetPtr();
            assert(obj_ptr != nullptr);
            assert(interface_ != nullptr);
            interface_->Delete(obj_ptr);
        }
    }

    TxKey(const TxKey &) = delete;

    TxKey &operator=(const TxKey &) = delete;

    TxKey &operator=(TxKey &&rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        // If "this" owns an object, de-allocates the object first.
        if (IsOwner())
        {
            void *obj_ptr = GetPtr();
            assert(obj_ptr != nullptr);
            assert(interface_ != nullptr);
            interface_->Delete(obj_ptr);
        }

        interface_ = rhs.interface_;
        obj_addr_ = rhs.obj_addr_;

        // The input loses the ownership, if it has any. So, its ownership bit
        // is 0 after the assignment.
        rhs.obj_addr_ = rhs.obj_addr_ & mask;

        return *this;
    }

    /*
    virtual TxKey &operator=(const TxKey &that) = 0;*/
    bool operator==(const TxKey &rhs) const
    {
        return interface_->Equal(GetConstPtr(), rhs.GetConstPtr());
    }

    bool operator<(const TxKey &rhs) const
    {
        return interface_->LessThan(GetConstPtr(), rhs.GetConstPtr());
    }

    bool operator<=(const TxKey &rhs) const
    {
        return *this < rhs || *this == rhs;
    }

    size_t Hash() const
    {
        return interface_->Hash(GetConstPtr());
    }

    void Serialize(std::string &str) const
    {
        interface_->SerializeStr(GetConstPtr(), str);
    }

    bool NeedsDefrag(mi_heap_t *heap) const
    {
        return interface_->NeedsDefrag(GetPtr(), heap);
    }

    std::string ToString() const
    {
        return interface_->ToString(GetConstPtr());
    }

    // Only used for single string field key, they do not need create a new
    // buffer and convert the value's type, then copy into new buffer.
    std::string_view KVSerialize() const
    {
        return interface_->KVSerialize(GetConstPtr());
    }
    // Deserialize the key from buffer. The buffer does not include the length
    // of key's . Here should use len as buffer's length.
    // void KVDeserialize(const char *buf, size_t len);

    /**
     * To estimate log length.
     * @return
     */
    size_t SerializedLength() const
    {
        return interface_->SerializedLen(GetConstPtr());
    }

    TxKey Clone() const
    {
        // When the key represents negative or positive infinity, returns a
        // shallow copy.
        return Type() == KeyType::Normal ? interface_->Clone(GetConstPtr())
                                         : GetShallowCopy();
    }

    // virtual std::string ToString() const = 0;

    void Copy(const TxKey &rhs) const
    {
        interface_->Copy(GetPtr(), rhs.GetConstPtr());
    }

    /**
     * @brief Returns a shallow copy of this object that only references and has
     * no ownership.
     *
     * @return TxKey
     */
    TxKey GetShallowCopy() const
    {
        TxKey copy;
        copy.obj_addr_ = reinterpret_cast<uint64_t>(GetPtr());
        copy.interface_ = interface_;

        return copy;
    }

    /**
     * Whether a search key is prefix of a full key, to distinguish between
     * prefix equality and full equality. Returns true if *this is a prefix of
     * rhs, false if *this and rhs is exactly the same. Should be called only
     * when *this == rhs returns true.
     * @param rhs
     * @return
     */
    // bool IsPrefixOf(const TxKey &rhs) const
    // {
    //     return is_prefix_func_(obj_ptr_, rhs.obj_ptr_);
    // }

    size_t MemUsage() const
    {
        return interface_->MemUsage(GetConstPtr());
    }

    void SetPackedKey(const char *data, size_t size)
    {
        interface_->SetPackedKey(GetPtr(), data, size);
    }

    const char *Data() const
    {
        const void *ptr = GetConstPtr();
        return ptr == nullptr ? nullptr : interface_->Data(GetConstPtr());
    }

    size_t Size() const
    {
        const void *ptr = GetConstPtr();
        return ptr == nullptr ? 0 : interface_->Size(GetConstPtr());
    }

    KeyType Type() const
    {
        return interface_ != nullptr ? interface_->Type(GetConstPtr())
                                     : KeyType::NegativeInf;
    }

    double PosInInterval(const KeySchema *key_schema,
                         const TxKey &min_key,
                         const TxKey &max_key) const
    {
        assert(min_key <= max_key);

        if (*this <= min_key)
        {
            return 0;
        }
        else if (max_key <= *this)
        {
            return 1;
        }
        else
        {
            return 0.5;
        }
    }

    const void *KeyPtr() const
    {
        return GetConstPtr();
    }

    void Clear()
    {
        obj_addr_ = 0;
    }

    static size_t HashCode(const TxKey &sk, const TxKey &pk)
    {
        size_t hash = 17;
        hash = hash * 23 + sk.Hash();
        hash = hash * 23 + pk.Hash();

        return hash;
    }

    bool IsOwner() const
    {
        return obj_addr_ & 1;
    }

    template <typename T>
    std::unique_ptr<T> MoveKey()
    {
        assert(IsOwner());
        T *key_ptr = static_cast<T *>(GetPtr());
        std::unique_ptr<T> key_uptr(key_ptr);
        obj_addr_ = obj_addr_ & mask;

        return key_uptr;
    }

    template <typename T>
    const T *GetKey() const
    {
        return static_cast<const T *>(GetConstPtr());
    }

    /**
     * @brief Releases the ownership by setting the ownership bit to 0. The
     * function is only used when TxKey is part of a union struct and prevents
     * move assignment to TxKey from inappropriately invoking de-allocation.
     *
     */
    void Release()
    {
        obj_addr_ &= mask;
    }

private:
    void *GetPtr() const
    {
        return reinterpret_cast<void *>(obj_addr_ & mask);
    }

    const void *GetConstPtr() const
    {
        return reinterpret_cast<const void *>(obj_addr_ & mask);
    }

public:
    const TxKeyInterface *interface_{nullptr};
    uint64_t obj_addr_{0};

    static constexpr uint64_t mask = UINT64_MAX - 1;
};

struct TxKeyHash
{
    std::size_t operator()(const TxKey *key_ptr) const
    {
        return key_ptr->Hash();
    }
};

template <typename T>
struct PtrEqual
{
    bool operator()(const T *lhs, const T *rhs) const
    {
        return *lhs == *rhs;
    }
};

template <typename T>
struct PtrLessThan
{
    bool operator()(const T *lhs, const T *rhs) const
    {
        return *lhs < *rhs;
    }
};

template <typename T>
struct CompositeHash
{
    static void Hash(size_t &hash, const T &val)
    {
        hash = hash * 23 + std::hash<T>()(val);
    }
};

template <typename... Types>
class CompositeKey
{
public:
    CompositeKey() : field_cnt_(0)
    {
    }

    CompositeKey(Types &&...val)
        : fields_(std::forward<Types>(val)...),
          field_cnt_(std::tuple_size<decltype(fields_)>::value)
    {
    }

    CompositeKey(std::tuple<Types...> &&t)
        : fields_(std::forward<std::tuple<Types...>>(t)),
          field_cnt_(std::tuple_size<decltype(fields_)>::value)
    {
    }

    CompositeKey(const CompositeKey &other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    CompositeKey(const CompositeKey &rhs, const txservice::KeySchema *schema)
        : fields_(rhs.fields_), field_cnt_(rhs.field_cnt_)
    {
    }

    CompositeKey(CompositeKey &&other) noexcept
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    ~CompositeKey() = default;

    void Reset(Types &&...vals)
    {
        TupleResetHelper(fields_,
                         std::index_sequence_for<Types...>{},
                         std::forward<Types>(vals)...);
    }

    void Reset(const Types &...vals)
    {
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    bool operator==(const CompositeKey<Types...> &rhs) const
    {
        if (&rhs == CompositeKey<Types...>::NegativeInfinity() ||
            &rhs == CompositeKey<Types...>::PositiveInfinity())
        {
            return this == &rhs;
        }
        return fields_ == rhs.fields_;
    }

    bool operator!=(const CompositeKey<Types...> &rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const CompositeKey<Types...> &rhs) const
    {
        if (this == CompositeKey<Types...>::PositiveInfinity() ||
            &rhs == CompositeKey<Types...>::NegativeInfinity())
        {
            return false;
        }
        else if (this == CompositeKey<Types...>::NegativeInfinity() ||
                 &rhs == CompositeKey<Types...>::PositiveInfinity())
        {
            return true;
        }

        return fields_ < rhs.fields_;
    }

    bool operator<=(const CompositeKey<Types...> &rhs) const
    {
        return *this < rhs || *this == rhs;
    }

    CompositeKey &operator=(const CompositeKey<Types...> &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        fields_ = rhs.fields_;
        field_cnt_ = rhs.field_cnt_;

        return *this;
    }

    size_t Hash() const
    {
        size_t hash = field_cnt_ <= 1 ? 0 : 17;

        std::apply([&hash](Types... field)
                   { (void(CompositeHash<Types>::Hash(hash, field)), ...); },
                   fields_);

        return hash;
    }

    std::string Print()
    {
        return std::apply([](Types... v)
                          { return ((Stringify<Types>::Get(v) + ",") + ...); },
                          fields_);
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const
    {
        size_t mem_size = std::apply(
            [](Types... field) { return (MemSize<Types>::Size(field) + ...); },
            fields_);

        if (buf.capacity() - offset < mem_size)
        {
            buf.resize(offset + mem_size);
        }

        std::apply(
            [&buf, &offset](Types... field)
            { (void(Serializer<Types>::Serialize(field, buf, offset)), ...); },
            fields_);
    }

    void Serialize(std::string &buf) const
    {
        std::apply([&buf](Types... field)
                   { (void(Serializer<Types>::Serialize(field, buf)), ...); },
                   fields_);
    }

    void Deserialize(const char *buf, size_t &offset, const KeySchema *schema)
    {
        TupleDeserializeHelper(
            fields_, std::index_sequence_for<Types...>{}, buf, offset);
    }

    size_t SerializedLength() const
    {
        return 0;
    }

    std::unique_ptr<CompositeKey<Types...>> Clone() const
    {
        return std::make_unique<CompositeKey<Types...>>(*this);
    }

    void Copy(const CompositeKey<Types...> &rhs)
    {
        fields_ = rhs.fields_;
        field_cnt_ = rhs.field_cnt_;
    }

    void SetPackedKey(const char *data, size_t len)
    {
        assert(false);
    }

    const std::tuple<Types...> &Tuple() const
    {
        return fields_;
    }

    std::tuple<Types...> &Tuple()
    {
        return fields_;
    }

    size_t MemUsage() const
    {
        return 0;
    }

    TxKey CloneTxKey() const
    {
        return TxKey();
    }

    KeyType Type() const
    {
        return KeyType::Normal;
    }

    const char *Data() const
    {
        return nullptr;
    }

    size_t Size() const
    {
        return 0;
    }

    std::string ToString() const
    {
        return std::apply(
            [](Types... v)
            { return "<" + ((Stringify<Types>::Get(v) + ",") + ...) + ">"; },
            fields_);
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    static const CompositeKey<Types...> *NegativeInfinity()
    {
        return &neg_inf;
    }

    static const CompositeKey<Types...> *PositiveInfinity()
    {
        return &pos_inf;
    }

    static const ::txservice::TxKeyInterface *TxKeyImpl()
    {
        static const txservice::TxKeyInterface tx_key_impl{
            *CompositeKey::NegativeInfinity()};
        return &tx_key_impl;
    }

    static CompositeKey<Types...> neg_inf;
    static CompositeKey<Types...> pos_inf;

private:
    std::tuple<Types...> fields_;
    size_t field_cnt_;
};

template <typename... Types>
CompositeKey<Types...> CompositeKey<Types...>::neg_inf =
    CompositeKey<Types...>();

template <typename... Types>
CompositeKey<Types...> CompositeKey<Types...>::pos_inf =
    CompositeKey<Types...>();

struct alignas(2) VoidKey
{
    VoidKey()
    {
    }

    ~VoidKey() = default;

    friend bool operator==(const VoidKey &lhs, const VoidKey &rhs)
    {
        const VoidKey *neg_ptr = VoidKey::NegativeInfinity();
        const VoidKey *pos_ptr = VoidKey::PositiveInfinity();

        if (&lhs == neg_ptr || &lhs == pos_ptr || &rhs == neg_ptr ||
            &rhs == pos_ptr)
        {
            return &lhs == &rhs;
        }

        // VoidKey is singleton, besides negative and positive
        // infinity.
        return true;
    }

    bool operator!=(const VoidKey &rhs) const
    {
        return !(*this == rhs);
    }

    friend bool operator<(const VoidKey &lhs, const VoidKey &rhs)
    {
        const VoidKey *neg_ptr = VoidKey::NegativeInfinity();
        const VoidKey *pos_ptr = VoidKey::PositiveInfinity();

        if (&lhs == neg_ptr)
        {
            // Negative infinity is less than any key, except
            // itself.
            return &rhs != neg_ptr;
        }
        else if (&lhs == pos_ptr || &rhs == neg_ptr)
        {
            return false;
        }
        else if (&rhs == pos_ptr)
        {
            // Positive infinity is greater than any key, except
            // itself.
            return &lhs != pos_ptr;
        }

        // VoidKey is singleton, besides negative and positive
        // infinity.
        return false;
    }

    friend bool operator<=(const VoidKey &lhs, const VoidKey &rhs)
    {
        return !(rhs < lhs);
    }

    size_t Hash() const
    {
        return 0;
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const
    {
    }

    void Serialize(std::string &str) const
    {
    }

    size_t SerializedLength() const
    {
        return 0;
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const KeySchema *key_schema)
    {
    }

    std::string_view KVSerialize() const
    {
        assert(false);
        return std::string_view();
    }
    void KVDeserialize(const char *buf, size_t len)
    {
        assert(false);
    }

    TxKey CloneTxKey() const
    {
        if (this == NegativeInfinity())
        {
            return TxKey(NegativeInfinity());
        }
        else if (this == PositiveInfinity())
        {
            return TxKey(PositiveInfinity());
        }
        else
        {
            return TxKey(std::make_unique<VoidKey>());
        }
    }

    bool IsPrefixOf(const VoidKey &rhs) const
    {
        return false;
    }

    void Copy(const VoidKey &rhs)
    {
    }

    std::string ToString() const
    {
        return std::string("");
    }

    size_t MemUsage() const
    {
        return 0;
    }

    void SetPackedKey(const char *data, size_t size)
    {
    }

    const char *Data() const
    {
        return nullptr;
    }

    size_t Size() const
    {
        return 0;
    }

    KeyType Type() const
    {
        if (this == VoidKey::NegativeInfinity())
        {
            return KeyType::NegativeInf;
        }
        else if (this == VoidKey::PositiveInfinity())
        {
            return KeyType::PositiveInf;
        }
        else
        {
            return KeyType::Normal;
        }
    }

    bool DefragIfNecessary(mi_heap_t *heap)
    {
        return false;
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    static const VoidKey *NegativeInfinity()
    {
        static const VoidKey neg_inf;
        return &neg_inf;
    }

    static const VoidKey *PositiveInfinity()
    {
        static const VoidKey pos_inf;
        return &pos_inf;
    }

    static const TxKey *NegInfTxKey()
    {
        static const TxKey neg_inf_tx_key{VoidKey::NegativeInfinity()};
        return &neg_inf_tx_key;
    }

    static const TxKey *PosInfTxKey()
    {
        static const TxKey pos_inf_tx_key{VoidKey::PositiveInfinity()};
        return &pos_inf_tx_key;
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface tx_key_impl{*VoidKey::NegativeInfinity()};
        return &tx_key_impl;
    }
};

class TxKeyFactory
{
    using CreateTxKeyFunc = TxKey (*)(const char *, size_t);
    using CreateDefaultTxKeyFunc = TxKey (*)();

public:
    static void RegisterNegInfTxKey(const TxKey *neg_inf_tx_key)
    {
        assert(Instance().neg_inf_tx_key_ == nullptr);
        Instance().neg_inf_tx_key_ = neg_inf_tx_key;
    }

    static const TxKey *NegInfTxKey()
    {
        assert(Instance().neg_inf_tx_key_ != nullptr);
        return Instance().neg_inf_tx_key_;
    }

    static void RegisterPosInfTxKey(const TxKey *pos_inf_tx_key)
    {
        assert(Instance().pos_inf_tx_key_ == nullptr);
        Instance().pos_inf_tx_key_ = pos_inf_tx_key;
    }

    static const TxKey *PosInfTxKey()
    {
        assert(Instance().pos_inf_tx_key_ != nullptr);
        return Instance().pos_inf_tx_key_;
    }

    static void RegisterPackedNegativeInfinity(
        const TxKey *packed_negative_infinity_key)
    {
        assert(Instance().packed_negative_infinity_key_ == nullptr);
        Instance().packed_negative_infinity_key_ = packed_negative_infinity_key;
    }

    static const TxKey *PackedNegativeInfinity()
    {
        assert(Instance().packed_negative_infinity_key_ != nullptr);
        return Instance().packed_negative_infinity_key_;
    }

    static void RegisterCreateTxKeyFunc(CreateTxKeyFunc create_tx_key_func)
    {
        assert(Instance().create_tx_key_func_ == nullptr);
        Instance().create_tx_key_func_ = create_tx_key_func;
    }

    static TxKey CreateTxKey(const char *data, size_t size)
    {
        assert(Instance().create_tx_key_func_ != nullptr);
        return Instance().create_tx_key_func_(data, size);
    }

    static void RegisterCreateDefaultTxKeyFunc(
        CreateDefaultTxKeyFunc create_default_tx_key_func)
    {
        assert(Instance().create_default_tx_key_func_ == nullptr);
        Instance().create_default_tx_key_func_ = create_default_tx_key_func;
    }

    static TxKey CreateDefaultTxKey()
    {
        assert(Instance().create_default_tx_key_func_ != nullptr);
        return Instance().create_default_tx_key_func_();
    }

private:
    static TxKeyFactory &Instance()
    {
        static TxKeyFactory tx_key_factory_;
        return tx_key_factory_;
    }

    CreateTxKeyFunc create_tx_key_func_{nullptr};
    CreateDefaultTxKeyFunc create_default_tx_key_func_{nullptr};

    const TxKey *neg_inf_tx_key_{nullptr};
    const TxKey *pos_inf_tx_key_{nullptr};
    // TODO(lzx): merge neg_inf_tx_key_ and packed_negative_infinity_key_ to
    // single
    const TxKey *packed_negative_infinity_key_{nullptr};
};

}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::TxKey *>
{
    std::size_t operator()(const txservice::TxKey *key) const
    {
        return key->Hash();
    }
};
}  // namespace std
