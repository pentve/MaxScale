#include <gtest/gtest.h>

#include <string>
#include <fstream>
#include <sstream>

extern "C"
{
    #include "../querylogfilter.c"
}

class LSTests : public testing::Test
{
protected:

    virtual void SetUp()
    {
        mTestLogName = "querylog_test.log";
    }
    
    virtual void TearDown()
    {
        remove(mTestLogName.c_str());
    }
    
    void AssertFileContent(const std::string& fileName, const std::string& expectedContent)
    {
        std::ifstream in(fileName.c_str());
        std::stringstream buf;
        buf << in.rdbuf();
        const std::string content = buf.str();
        ASSERT_STREQ(expectedContent.c_str(), content.c_str());
    }

    std::string mTestLogName;
};

TEST_F(LSTests, TestResetCounter)
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

TEST_F(LSTests, TestUpdateCounter)
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

TEST_F(LSTests, TestLogWrite)
{
    LS_SESSION session;
    bindCounters(&session);
    resetCounters(&session);
    session.timestamp = (unsigned long) time(NULL);
    session.fp = fopen(mTestLogName.c_str(), "w");
    writeLogIfNeeded(&session, 0);
    fclose(session.fp);
    char t_str[1024];
    getTimestampAsDateTime(session.timestamp, t_str, sizeof(t_str));
    std::string expected_content = std::string(t_str) + "," + std::string(t_str) + ",0,0,0,0\n";
    AssertFileContent(mTestLogName, expected_content);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
