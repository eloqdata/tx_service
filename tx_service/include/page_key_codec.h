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

// Page-row key codec for paged large objects (eloqkv docs/08-paged-objects.md
// §5). This is ON-DISK FORMAT: once a paged row exists in any store, the
// encoding below can never change. It is the single shared encoder the design
// requires — the fetch path, the flush path, and the store-side sweeper must
// all include this header rather than reimplementing the layout, so the read
// and write sides cannot disagree about row identity.
//
// Layout (byte-exact):
//
//   \x00 'E' 'K' 'V' 'P' 'A' 'G' 'E'  <key_len:4, big-endian>
//   <object key bytes>  <kind:1>  <page_id:4, big-endian>
//
// - The 8-byte magic makes an accidental collision with a binary user key
//   2^-64 instead of 1/256; the protocol layer additionally REJECTS user keys
//   beginning with it, making the rate exactly zero.
// - The FIXED-WIDTH length prefix makes the object component PREFIX-FREE,
//   which is what every locality claim rests on. Without it, key "a" is a
//   prefix of key "a\0...", their page rows interleave, and a per-object
//   prefix delete removes another object's pages. With it: keys of different
//   lengths separate at the length field, keys of equal length are the same
//   fixed width, so [magic][len][key] identifies exactly one object and one
//   object's page rows are contiguous. The length field is 4 bytes so it
//   spans every key the largest-key backend admits (RocksDB's 32 MB), not
//   just small keys — reserving the codec overhead must not shrink the
//   public key limit on any backend.
// - Metadata rows are NOT part of this keyspace: they live under the plain
//   object key. Nothing requires an object's metadata row to be adjacent to
//   its page rows, and it is not.
// - Big-endian page id so one object's page rows sort in page order.
// - The embedded object key is the full engine key (namespace prefix
//   included), so page rows inherit namespace isolation. The protocol
//   layer's public key limit reserves this codec's overhead below the
//   store's key ceiling (docs/08 §5 "Key-length budget").

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace txservice
{
inline constexpr size_t kPageKeyMagicLen = 8;
inline constexpr char kPageKeyMagic[kPageKeyMagicLen] = {
    '\x00', 'E', 'K', 'V', 'P', 'A', 'G', 'E'};
inline constexpr size_t kPageKeyLenFieldLen = 4;
// kind(1) + page_id(4).
inline constexpr size_t kPageKeySuffixLen = 5;
// Total non-key bytes a page key adds to its object key. The protocol
// layer's public key-length limit must reserve at least this much headroom
// below the store's key ceiling.
inline constexpr size_t kPageKeyOverhead =
    kPageKeyMagicLen + kPageKeyLenFieldLen + kPageKeySuffixLen;
// The object key may itself be empty only if the engine permits empty keys,
// so this is the minimum valid page-key size.
inline constexpr size_t kPageKeyMinLen = kPageKeyOverhead;

/**
 * @brief The two page-row types sharing the reserved keyspace (docs/08 §5).
 */
enum class PageRowKind : uint8_t
{
    // A slotted hash page (docs/08 §4 layout).
    HashPage = 0,
    // A raw out-of-line large-value data page (no header).
    LargeValuePage = 1,
};

/**
 * @brief True iff key begins with the reserved magic. Used both to route
 * store-side scans and to reject user keys at command parsing.
 */
inline bool HasPageKeyMagic(std::string_view key)
{
    return key.size() >= kPageKeyMagicLen &&
           std::memcmp(key.data(), kPageKeyMagic, kPageKeyMagicLen) == 0;
}

/**
 * @brief Appends the per-object page-key PREFIX — [magic][len][key] — the
 * exact byte range covering all of one object's page rows and nothing else
 * (see the prefix-freeness note above). This is what a range/prefix delete
 * or a per-object store scan keys on.
 * @param object_key The full engine key bytes, namespace prefix included.
 */
inline void EncodePageKeyPrefix(std::string &out, std::string_view object_key)
{
    assert(object_key.size() <= UINT32_MAX);
    out.reserve(out.size() + kPageKeyMagicLen + kPageKeyLenFieldLen +
                object_key.size());
    out.append(kPageKeyMagic, kPageKeyMagicLen);
    uint32_t len = static_cast<uint32_t>(object_key.size());
    out.push_back(static_cast<char>((len >> 24) & 0xFF));
    out.push_back(static_cast<char>((len >> 16) & 0xFF));
    out.push_back(static_cast<char>((len >> 8) & 0xFF));
    out.push_back(static_cast<char>(len & 0xFF));
    out.append(object_key.data(), object_key.size());
}

/**
 * @brief Appends the encoded page key for <object_key, kind, page_id> to out.
 * @param object_key The full engine key bytes, namespace prefix included.
 */
inline void EncodePageKey(std::string &out,
                          std::string_view object_key,
                          PageRowKind kind,
                          uint32_t page_id)
{
    EncodePageKeyPrefix(out, object_key);
    out.push_back(static_cast<char>(kind));
    // Big-endian page id.
    out.push_back(static_cast<char>((page_id >> 24) & 0xFF));
    out.push_back(static_cast<char>((page_id >> 16) & 0xFF));
    out.push_back(static_cast<char>((page_id >> 8) & 0xFF));
    out.push_back(static_cast<char>(page_id & 0xFF));
}

/**
 * @brief The decoded components of a page key. object_key_ is a view into
 * the encoded bytes and is valid only while they live.
 */
struct PageKeyParts
{
    std::string_view object_key_;
    PageRowKind kind_{PageRowKind::HashPage};
    uint32_t page_id_{0};
};

/**
 * @brief Decodes an encoded page key. The length field fixes every offset,
 * and a valid page key's total size is fully determined by it, so the check
 * is exact.
 * @return false if the bytes do not carry the magic, do not have exactly the
 * size the length field implies, or hold a malformed kind byte — store-side
 * scans can treat any false return as "not a page row".
 */
inline bool DecodePageKey(std::string_view key, PageKeyParts &parts)
{
    if (key.size() < kPageKeyMinLen || !HasPageKeyMagic(key))
    {
        return false;
    }
    const auto *bytes = reinterpret_cast<const unsigned char *>(key.data());
    uint32_t key_len =
        (static_cast<uint32_t>(bytes[kPageKeyMagicLen]) << 24) |
        (static_cast<uint32_t>(bytes[kPageKeyMagicLen + 1]) << 16) |
        (static_cast<uint32_t>(bytes[kPageKeyMagicLen + 2]) << 8) |
        static_cast<uint32_t>(bytes[kPageKeyMagicLen + 3]);
    if (key.size() != kPageKeyOverhead + static_cast<size_t>(key_len))
    {
        return false;
    }
    size_t kind_pos = kPageKeyMagicLen + kPageKeyLenFieldLen + key_len;
    uint8_t kind = bytes[kind_pos];
    if (kind > static_cast<uint8_t>(PageRowKind::LargeValuePage))
    {
        return false;
    }
    parts.kind_ = static_cast<PageRowKind>(kind);
    parts.page_id_ = (static_cast<uint32_t>(bytes[kind_pos + 1]) << 24) |
                     (static_cast<uint32_t>(bytes[kind_pos + 2]) << 16) |
                     (static_cast<uint32_t>(bytes[kind_pos + 3]) << 8) |
                     static_cast<uint32_t>(bytes[kind_pos + 4]);
    parts.object_key_ =
        key.substr(kPageKeyMagicLen + kPageKeyLenFieldLen, key_len);
    return true;
}
}  // namespace txservice
