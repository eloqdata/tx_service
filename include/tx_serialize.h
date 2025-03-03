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

#include <stdint.h>

#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace txservice
{
template <typename... Types, std::size_t... Is>
void TupleResetHelper(std::tuple<Types...> &dest,
                      std::index_sequence<Is...>,
                      const Types &...vals)
{
    (void(std::get<Is>(dest) = vals), ...);
}

template <typename T>
struct Stringify
{
    static std::string Get(T &val)
    {
        return typeid(T).name();
    }
};

template <>
struct Stringify<int>
{
    static std::string Get(int &val)
    {
        return std::to_string(val);
    }
};

template <>
struct Stringify<double>
{
    static std::string Get(double &val)
    {
        return std::to_string(val);
    }
};

template <>
struct Stringify<std::string>
{
    static std::string Get(std::string &val)
    {
        return val;
    }
};

template <typename T>
struct MemSize
{
    static size_t Size(const T &)
    {
        return sizeof(T);
    }
};

template <>
struct MemSize<std::string>
{
    static size_t Size(const std::string &val)
    {
        // Uses 2 bytes to represent the string length. Almost all SQL runtime
        // and storage engines have a limit on the key length. 65536 bytes are
        // big enough for SQL primary/secondary keys.
        return val.length() + 2;
    }
};

template <typename T>
struct Serializer
{
    static void Serialize(const T &val, std::vector<char> &buf, size_t &offset)
    {
        size_t len = MemSize<T>::Size(val);
        const char *val_ptr =
            static_cast<const char *>(static_cast<const void *>(&val));
        std::copy(val_ptr, val_ptr + len, buf.begin() + offset);

        offset += len;
    }

    static void Serialize(const T &val, std::string &buf)
    {
        size_t len = MemSize<T>::Size(val);
        const char *val_ptr =
            static_cast<const char *>(static_cast<const void *>(&val));

        buf.append(val_ptr, len);
    }

    static T Deserialize(const char *buf, size_t &offset)
    {
        const T *tptr = (const T *) (buf + offset);
        offset += MemSize<T>::Size(*tptr);

        return *tptr;
    }
};

template <>
struct Serializer<std::string>
{
    static void Serialize(const std::string &val_str,
                          std::vector<char> &buf,
                          size_t &offset)
    {
        if (val_str.length() > UINT16_MAX)
        {
            throw std::runtime_error(
                "The length of the string to be serialized exceeds 2 bytes.");
        }
        uint16_t str_len =
            (uint16_t) val_str.length();  // Should check the string length

        Serializer<uint16_t>::Serialize(str_len, buf, offset);

        std::copy(val_str.begin(), val_str.end(), buf.begin() + offset);

        offset += MemSize<std::string>::Size(val_str);
    }

    static void Serialize(const std::string &val_str, std::string &buf)
    {
        if (val_str.length() > UINT16_MAX)
        {
            throw std::runtime_error(
                "The length of the string to be serialized exceeds 2 bytes.");
        }
        uint16_t str_len =
            (uint16_t) val_str.length();  // Should check the string length

        Serializer<uint16_t>::Serialize(str_len, buf);

        buf.append(val_str.data(), str_len);
    }

    static std::string Deserialize(const char *buf, size_t &offset)
    {
        uint16_t str_len = Serializer<uint16_t>::Deserialize(buf, offset);
        std::string str(buf + offset, str_len);
        offset += str_len;

        return str;
    }
};

template <typename... Types, std::size_t... Is>
void TupleDeserializeHelper(std::tuple<Types...> &dest,
                            std::index_sequence<Is...>,
                            const char *buf,
                            size_t &start)
{
    (void(std::get<Is>(dest) = Serializer<Types>::Deserialize(buf, start)),
     ...);
}

// Serialize helpers
template <typename T>
void serialize_to(const T *v, std::vector<char> &buf, size_t &offset)
{
    const char *v_char_ptr =
        static_cast<const char *>(static_cast<const void *>(v));
    std::copy(v_char_ptr, v_char_ptr + sizeof(T), buf.begin() + offset);
    offset += sizeof(T);
}

template void serialize_to(const uint64_t *v,
                           std::vector<char> &buf,
                           size_t &offset);
template void serialize_to(const int32_t *v,
                           std::vector<char> &buf,
                           size_t &offset);
template void serialize_to(const uint16_t *v,
                           std::vector<char> &buf,
                           size_t &offset);
template void serialize_to(const uint8_t *v,
                           std::vector<char> &buf,
                           size_t &offset);

template <typename T>
void SerializeToStr(const T *v, std::string &str)
{
    const char *v_ptr = static_cast<const char *>(static_cast<const void *>(v));
    str.append(v_ptr, sizeof(T));
}

template void SerializeToStr(const uint64_t *v, std::string &str);
template void SerializeToStr(const int32_t *v, std::string &str);
template void SerializeToStr(const uint16_t *v, std::string &str);
template void SerializeToStr(const uint8_t *v, std::string &str);

// Deserialize helpers
template <typename T>
void DesrializeFrom(const char *buf, size_t &offset, T *v)
{
    const T *ptr = reinterpret_cast<const T *>(buf + offset);
    *v = *ptr;
    offset += sizeof(T);
}

template void DesrializeFrom(const char *buf, size_t &offset, uint64_t *v);
template void DesrializeFrom(const char *buf, size_t &offset, int32_t *v);
template void DesrializeFrom(const char *buf, size_t &offset, uint16_t *v);
template void DesrializeFrom(const char *buf, size_t &offset, uint8_t *v);

}  // namespace txservice
