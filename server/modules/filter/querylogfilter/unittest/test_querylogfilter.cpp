#include <gtest/gtest.h>

#include <string>
#include <fstream>
#include <sstream>

extern "C"
{
    #include "../querylogfilter.c"
}

// Unit tests for querylogfilter.c
//
// Note: memory management of test data neglected on purpose

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
    
    void AssertFileContent(const std::string& fileName, const std::string& expectedContent) const
    {
        std::ifstream in(fileName.c_str());
        std::stringstream buf;
        buf << in.rdbuf();
        const std::string content = buf.str();
        ASSERT_STREQ(expectedContent.c_str(), content.c_str());
    }

    FILTER_PARAMETER** GetParameters(char* fileBaseIn, char* intervalIn) const
    {
        FILTER_PARAMETER** p = new FILTER_PARAMETER*[3];
        p[0] = new FILTER_PARAMETER();
        p[1] = new FILTER_PARAMETER();
        p[2] = NULL;
        if (fileBaseIn)
        {
            p[0]->name = "filebase"; p[0]->value = fileBaseIn;
        }
        else
        {
            p[0]->name = "unknown";
        }
        if (intervalIn)
        {
            p[1]->name = "interval"; p[1]->value = intervalIn;
        }
        else
        {
            p[1]->name = "unknown";
        }
        return p;
    }

    void AssertParameters(bool expectedResult,
        const char* expectedFileBase,
        const unsigned int expectedInterval,
        char* fileBaseIn,
        char* intervalIn) const
    {
        char* buffer = new char[1024];
	unsigned int interval = 60; // default

        FILTER_PARAMETER** p = GetParameters(fileBaseIn, intervalIn);

        const KnownParam knownParams[] =
        {
            { .var = (void**) &buffer, .type = param_text, .name = "filebase", .mandatory = true },
            { .var = (void**) &interval, .type = param_natural_number, .name = "interval", .mandatory = false }
        };

        const bool result = parseParameters((FILTER_PARAMETER**) p, knownParams,
            sizeof(knownParams)/sizeof(knownParams[0]),
            "some name");

        ASSERT_EQ(expectedResult, result);
        if (expectedResult)
        {
            ASSERT_EQ(expectedInterval, interval);
            ASSERT_STREQ(expectedFileBase, buffer);
        }

        LS_INSTANCE* i = (LS_INSTANCE*) createInstance(NULL, p);
        if (expectedResult)
        {
            ASSERT_EQ(expectedInterval, i->loggingInterval);
            ASSERT_STREQ(expectedFileBase, i->filebase);
        }
        else
        {
            ASSERT_EQ(NULL, i);
        }
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

TEST_F(LSTests, TestParseParameters_ok)
{
    AssertParameters(true, "test", 1000, "test", "1000");
}

TEST_F(LSTests, TestParseParameters_intervalMissing_ok_default_60)
{
    AssertParameters(true, "some/path", 60, "some/path", NULL);
}

TEST_F(LSTests, TestParseParameters_fileBaseMissing_fail)
{
    AssertParameters(false, "", 0, NULL, "60");
}

TEST_F(LSTests, TestParseParameters_negativeInterval_fail)
{
    AssertParameters(false, "", 0, "test", "-1");
}

TEST_F(LSTests, TestNewInstance)
{
    FILTER_PARAMETER** p = GetParameters((char*) mTestLogName.c_str(), "1");
    LS_INSTANCE* i = (LS_INSTANCE*) createInstance(NULL, p);
    unsigned long start_time = (unsigned long) time(NULL);
    LS_SESSION* s = (LS_SESSION*) newSession((void**) i, NULL);
    unsigned long end_time = (unsigned long) time(NULL);
    ASSERT_STREQ(std::string(mTestLogName + ".0").c_str(), s->filename);
    ASSERT_GE(start_time, s->timestamp);
    ASSERT_LE(end_time, s->timestamp);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
