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
#include <string>

/** @brief
 Table / Index / Unique_Index naming convention for txservice
 Table name: ./DB/Table
 Index name: ./DB/Table*$$Index
 Unique_Index name: ./DB/Table*~~Unique_Index
 */
namespace txservice
{
inline const std::string INDEX_NAME_PREFIX = "*$$";
inline const std::string UNIQUE_INDEX_NAME_PREFIX = "*~~";

static constexpr int64_t UNKNOWN_TERM = -3;
static constexpr int64_t SKIP_CHECK_TERM = -2;
static constexpr int64_t INIT_TERM = -1;

static constexpr uint32_t UPLOAD_BATCH_SIZE = 409600;
}  // namespace txservice
