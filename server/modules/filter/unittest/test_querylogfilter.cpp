#include "../querylogfilter.c"
#include <gtest/gtest.h>

TEST(LS_Tests, TestTest)
{
    LS_SESSION session;
    resetCounters(&session);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
