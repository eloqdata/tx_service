#include <catch2/catch_all.hpp>

#include "harness/test_node.h"

using namespace txservice;
using namespace txservice::test;

TEST_CASE("TestNode brings up and tears down cleanly", "[testnode]")
{
    TestNode node(TestNodeOptions{}.CoreNum(2));
    auto t = node.BeginTx();
    REQUIRE(t.Commit());  // empty transaction commits
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
