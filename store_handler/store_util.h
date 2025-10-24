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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// #include "ds_request.pb.h"
#include "store_handler/kv_store.h"
#include "tx_service/include/cc/cc_entry.h"  // FlushRecord

// TODO(lzx): check this file is used, if not, remove it.

namespace EloqShare
{
std::string uint32_str_converter(uint32_t val);
std::string uint16_str_converter(uint16_t val);
std::string string_str_converter(const std::string &val);
std::string bool_str_converter(bool val);

uint32_t str_uint32_converter(const std::string &s);
uint16_t str_uint16_converter(const std::string &s);
std::string str_string_converter(const std::string &s);
bool str_bool_converter(const std::string &s);

template <typename T>
std::string SerializeVectorToString(
    const std::vector<T> &vec,
    const std::string &sep,
    std::function<std::string(const T &)> converter)
{
    std::string str;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        str.append(converter(vec[i]));
        if (i < vec.size() - 1)
        {
            str.append(sep);
        }
    }
    return str;
}

template <typename T>
void DeserializeStringToVector(const std::string &str,
                               const std::string &sep,
                               std::vector<T> &vec,
                               std::function<T(const std::string &)> converter)
{
    if (str.empty())
    {
        return;  // Return early if the string is empty
    }

    std::string::size_type start = 0;
    std::string::size_type end = str.find(sep);
    while (end != std::string::npos)
    {
        vec.push_back(converter(str.substr(start, end - start)));
        start = end + sep.size();
        end = str.find(sep, start);
    }

    // Handle the last element or the case where str has only one element
    vec.push_back(converter(str.substr(start)));
}

template <typename T>
void SerializeVector(const std::vector<T> &vec, std::string &str)
{
    // The vector size
    size_t cnt = vec.size();
    size_t val_len = sizeof(cnt);
    const char *val_ptr = reinterpret_cast<const char *>(&cnt);
    str.append(val_ptr, val_len);

    // The vector content
    val_len = sizeof(T);
    for (size_t i = 0; i < vec.size(); ++i)
    {
        const T &val = vec[i];
        val_ptr = reinterpret_cast<const char *>(&val);
        str.append(val_ptr, val_len);
    }
}

template <>
void SerializeVector<std::string>(const std::vector<std::string> &vec,
                                  std::string &str);

template <typename T>
void DeserializeToVector(const char *str, size_t length, std::vector<T> &vec)
{
    if (!length)
    {
        return;
    }

    size_t offset = 0;
    // The vector size.
    size_t vec_size = *(reinterpret_cast<const size_t *>(str + offset));
    offset += sizeof(size_t);

    // The vector content
    for (size_t i = 0; i < vec_size; ++i)
    {
        vec.emplace_back(*reinterpret_cast<const T *>(str + offset));
        offset += sizeof(T);
    }

    assert(offset == length);
}

template <>
void DeserializeToVector<std::string>(const char *str,
                                      size_t length,
                                      std::vector<std::string> &vec);

// void SerializeFlushRecord(const txservice::FlushRecord &flush_rec,
//                           std::vector<char> &buf);
// void SerializeFlushRecord(const txservice::FlushRecord &flush_rec,
//                           std::string &rec_str);

template <typename T>
T swap_endian(T x)
{
    T val = x;
    uint8_t *byte = reinterpret_cast<uint8_t *>(&val);
    size_t size = sizeof(T);
    for (size_t i = 0; i < size / 2; ++i)
    {
        std::swap(byte[i], byte[size - 1 - i]);
    }
    return val;
}

template <typename T, std::enable_if_t<std::is_unsigned<T>::value, bool> = true>
T host_to_big_endian(T val)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return swap_endian(val);
#else
    return val;
#endif
}

template <typename T, std::enable_if_t<std::is_unsigned<T>::value, bool> = true>
T big_endian_to_host(T val)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return swap_endian(val);
#else
    return val;
#endif
}

}  // namespace EloqShare
