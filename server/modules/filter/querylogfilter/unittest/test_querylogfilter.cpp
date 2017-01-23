#include <gtest/gtest.h>

extern "C"
{
    #include "../querylogfilter.c"
}

TEST(LS_Tests, TestResetCounter)
{
    LS_SESSION session;
    session.counters.selectQuery = 5;
    session.counters.insertQuery = 1;
    session.counters.updateQuery = 3;
    session.counters.deleteQuery = 7;
    resetCounters(&session);
    ASSERT_EQ(0, session.counters.selectQuery);
    ASSERT_EQ(0, session.counters.insertQuery);
    ASSERT_EQ(0, session.counters.updateQuery);
    ASSERT_EQ(0, session.counters.deleteQuery);
}

TEST(LS_Tests, TestUpdateCounter)
{
    LS_SESSION session;
    bindCounters(&session);
    resetCounters(&session);
    updateCounters(&session, "seLecT * from test;");
    ASSERT_EQ(1, session.counters.selectQuery);
    updateCounters(&session, "SELECT;"); // Note: invalid SQL is counted, too
    updateCounters(&session, "select * from test;");
    ASSERT_EQ(3, session.counters.selectQuery);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
