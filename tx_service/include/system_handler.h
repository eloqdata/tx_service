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

#include <functional>

namespace txservice
{
// This is used to callback functions in app server, such as MariaDB, on every
// node.
class SystemHandler
{
public:
    virtual ~SystemHandler() = default;

    // C++ std::future doesn't contain a then() method. Pass the continuation as
    // the done callback.
    virtual void ReloadCache(std::function<void(bool)> done)
    {
        done(true);
    }

    virtual void Shutdown()
    {
    }
};
}  // namespace txservice
