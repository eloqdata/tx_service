#pragma once

#include "system_handler.h"

namespace txservice
{
class MockSystemHandler : public txservice::SystemHandler
{
public:
    MockSystemHandler &Instance()
    {
        static MockSystemHandler instance;
        return instance;
    }

private:
    MockSystemHandler() = default;
    ~MockSystemHandler() = default;
};
}  // namespace txservice
