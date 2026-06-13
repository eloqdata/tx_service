#include <catch2/catch_all.hpp>

TEST_CASE("MemDataStore placeholder", "[mem-data-store]")
{
    REQUIRE(true);  // Real tests added in Part B.
}

// This target links Catch2::Catch2 (no bundled main); provide our own main so
// command-line flags can be parsed before brpc is used by later Part B tests.
int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
