/**
 */

/**
 * @file querylogfilter.c -Query logger/statistics
 * @verbatim
 *
 * Counts statistics for the basic query commands and logs them by a given interval
 * Counted query types:
 *   SELECT
 *   INSERT
 *   UPDATE
 *   DELETE
 * 
 * Configuration parameters:
 *   filebase [mandatory] -- the base of the filename, to which the session
 *                           number is appended
 *   interval [optional]  -- the logging interval in seconds (defaults
 *                           to 60 seconds)
 *
 * The filter makes no attempt to deal with query packets that do not fit
 * in a single GWBUF.
 * 
 * @endverbatim
 */

#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <filter.h>
#include <modinfo.h>
#include <modutil.h>
#include <log_manager.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <atomic.h>

MODULE_INFO info =
{
    MODULE_API_FILTER,
    MODULE_GA,
    FILTER_VERSION,
    "Query logger/statistics"
};

static char* version_str = "V0.9.0";

/**
 * The filter entry points
 */
static FILTER* createInstance(char**, FILTER_PARAMETER**);
static void* newSession(FILTER*, SESSION*);
static void closeSession(FILTER*, void*);
static void freeSession(FILTER*, void*);
static void setDownstream(FILTER*, void*, DOWNSTREAM*);
static int routeQuery(FILTER*, void*, GWBUF*);
static void diagnostic(FILTER*, void*, DCB*);

static FILTER_OBJECT MyObject =
{
    createInstance,
    newSession,
    closeSession,
    freeSession,
    setDownstream,
    NULL, // No Upstream requirement
    routeQuery,
    NULL, // No client reply
    diagnostic,
};

typedef struct
{
    unsigned int selectQuery;
    unsigned int insertQuery;
    unsigned int updateQuery;
    unsigned int deleteQuery;
} QueryCounters;

typedef struct
{
    unsigned int* counter;
    const char*   queryType;
} CounterBinding;

/**
 * Instance context
 */
typedef struct
{
    int sessions;                 /* The count of sessions */
    char *filebase;               /* The filename base */
    unsigned int loggingInterval; /* Log writing interval in seconds */
    char* myName;                 /* module name */
} LS_INSTANCE;

/**
 * Session context
 */
typedef struct
{
    DOWNSTREAM down;               /* downstream */
    char *filename;                /* the name of the log file */
    FILE *fp;                      /* file pointer to the log */
    unsigned long timestamp;       /* the start of the current logging interval */
    QueryCounters counters;        /* command counters */
    CounterBinding counterBind[4]; /* query command -> counter binding */
} LS_SESSION;

typedef enum { param_text, param_number, param_natural_number } ParamType;

typedef struct
{
    void**      var;
    ParamType   type;
    const char* name;
    bool        mandatory;
} KnownParam;

static bool parseParameters(FILTER_PARAMETER**, const KnownParam[], unsigned int, const char*);
static void getTimestampAsDateTime(unsigned int, char*, const unsigned int);
static void writeLogIfNeeded(LS_SESSION*, unsigned long);
static int passQueryDownstream(LS_SESSION*, GWBUF*);
static void updateCounters(LS_SESSION*, const char*);
static void resetCounters(LS_SESSION*);
static void bindCounters(LS_SESSION*);

/**
 * The mandatory version entry point
 *
 * @return version string of the module
 */
char*
version()
{
    return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 * @see function load_module in load_utils.c for explanation of lint
 */
/*lint -e14 */
void
ModuleInit()
{
}
/*lint +e14 */

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
FILTER_OBJECT*
GetModuleObject()
{
    return &MyObject;
}

/**
 * Create an instance of the filter for a particular service
 * within MaxScale.
 *
 * @param options   Options for this filter [unused]
 * @param params    Array of name/value pair parameters for the filter
 *
 * @return Instance context
 */
static FILTER*
createInstance(char** options, FILTER_PARAMETER** params)
{
    (void) options;
    LS_INSTANCE* instance;

    if (params == NULL)
    {
        return (FILTER*) NULL;
    }

    if ((instance = (LS_INSTANCE*) malloc(sizeof(LS_INSTANCE))) == NULL)
    {
        return (FILTER*) NULL;
    }

    instance->filebase = NULL;
    instance->loggingInterval = 60; /* default 1 minute */
    instance->myName = strdup(basename(__FILE__));
    instance->sessions = 0;

    const KnownParam known_params[] =
    {
        { .var = (void**) &instance->filebase, .type = param_text, .name = "filebase", .mandatory = true },
        { .var = (void**) &instance->loggingInterval, .type = param_natural_number, .name = "interval", .mandatory = false }
    };

    const bool parse_ok = parseParameters(params, known_params,
        sizeof(known_params) / sizeof(known_params[0]), instance->myName);
    if (!parse_ok)
    {
        free(instance->filebase);
        free(instance->myName);
        free(instance);
        return (FILTER*) NULL;
    }

    return (FILTER*) instance;
}

/**
 * Associate a new session with this instance of the filter.
 *
 * Create the file to log to and open it.
 *
 * @param instance_in  Filter instance context
 * @param session_in   Session context
 * 
 * @return Session context
 */
static void*
newSession(FILTER* instance_in, SESSION* session_in)
{
    LS_INSTANCE* instance = (LS_INSTANCE*) instance_in;
    LS_SESSION* session;

    if ((session = (LS_SESSION*) calloc(1, sizeof(LS_SESSION))) == NULL)
    {
        return NULL;
    }

    if ((session->filename = (char*) malloc(strlen(instance->filebase) + 64)) == NULL)
    {
        free(session);
        return NULL;
    }

    sprintf(session->filename, "%s.%d", instance->filebase, instance->sessions);

    // Multiple sessions can try to update instance->sessions simultaneously
    atomic_add(&(instance->sessions), 1);

    session->fp = fopen(session->filename, "w");
    if (session->fp == NULL)
    {
        MXS_ERROR("%s: failed to open file '%s' (%d)",
                  instance->myName, session->filename, errno);
        free(session->filename);
        free(session);
        return NULL;
    }

    fprintf(session->fp, "LogStart,LogEnd,SelectCount,InsertCount,UpdateCount,DeleteCount\n");
    fflush(session->fp); /* writing to disk by 'loggingInterval' shouldn't be too much */
    bindCounters(session);
    session->timestamp = (unsigned long) time(NULL);
    resetCounters(session);
    return session;
}

/**
 * Close session
 * Closes the file descriptor
 *
 * @param instance_in  Filter instance context [unused]
 * @param session_in   Session context
 */
static void
closeSession(FILTER* instance_in, void *session_in)
{
    (void) instance_in;

    LS_SESSION* session = (LS_SESSION*) session_in;
    if (session->fp)
    {
        writeLogIfNeeded(session, 0); /* write now */
        fclose(session->fp);
    }
}

/**
 * Free the memory associated with the session
 *
 * @param instance_in  Filter instance context
 * @param session_in   Session context
 */
static void
freeSession(FILTER* instance_in, void* session_in)
{
    LS_SESSION* session = (LS_SESSION*) session_in;
    free(session->filename);
    free(session);
    return;
}

/**
 * Set the downstream filter or router to which queries will be
 * passed from this filter.
 *
 * @param instance_in   Filter instance context
 * @param session_in    Session context
 * @param downstream  Downstream module
 */
static void
setDownstream(FILTER* instance_in, void* session_in, DOWNSTREAM* downstream)
{
    LS_SESSION* session = (LS_SESSION*) session_in;
    session->down = *downstream;
}

/**
 * The routeQuery entry point. This is passed the query buffer
 * to which the filter should be applied. Once applied the
 * query should normally be passed to the downstream component
 * (filter or router) in the filter chain.
 *
 * @param instance_in  Filter instance context
 * @param session_in   Session context
 * @param queue        Query data
 */
static int
routeQuery(FILTER* instance_in, void* session_in, GWBUF* queue)
{
    LS_INSTANCE* instance = (LS_INSTANCE*) instance_in;
    LS_SESSION* session = (LS_SESSION*) session_in;
    char* query_str = NULL;

    if (queue->next != NULL)
    {
        queue = gwbuf_make_contiguous(queue);
    }

    if ((query_str = modutil_get_SQL(queue)) == NULL)
    {
        return passQueryDownstream(session, queue);
    }

    trim(squeeze_whitespace(query_str));
    updateCounters(session, (const char*) query_str);
    writeLogIfNeeded(session, instance->loggingInterval);
    free(query_str);

    return passQueryDownstream(session, queue);
}

/**
 * Diagnostics routine
 *
 * If session is NULL then print diagnostics on the filter
 * instance as a whole, otherwise print diagnostics for the
 * particular session.
 *
 * @param   instance_in   Filter instance context
 * @param   session_in    Session context (may be NULL)
 * @param   dcb           DCB for diagnostic output
 */
static void
diagnostic(FILTER* instance_in, void* session_in, DCB* dcb)
{
    LS_INSTANCE* instance = (LS_INSTANCE*) instance_in;
    LS_SESSION* session = (LS_SESSION*) session_in;

    if (!session)
    {
        return;
    }

    dcb_printf(dcb, "\t\tLogging to file            %s.\n", session->filename);
}

/* Private functions */

/**
 * Parses the given input parameters against known parameters
 * and stores the result as pointed by 'knownParams'
 * 
 * @param params          Array of name/value pair parameters for the filter
 * @param knownParams     Structure of known parameters
 * @param knownParamCount Number of known parameters
 * @param myName          Module name (for logging)
 * 
 * @return successful or not
*/
static bool
parseParameters(FILTER_PARAMETER** params, const KnownParam knownParams[], unsigned int knownParamCount, const char* myName)
{
    for (unsigned int known_param_idx = 0; known_param_idx < knownParamCount; ++known_param_idx)
    {
        bool found = false;
        const KnownParam* known_param = &knownParams[known_param_idx];
        for (unsigned int input_param_idx = 0; params[input_param_idx]; ++input_param_idx)
        {
            FILTER_PARAMETER* input_param = params[input_param_idx];
            if (!strcmp(input_param->name, known_param->name))
            {
                if (known_param->type == param_text)
                {
                    *((char**) known_param->var) = (char*) strdup(input_param->value);
                    found = true;
                    break;
                }
                else if (known_param->type == param_number)
                {
                    *((int*) known_param->var) = (int) atoi(input_param->value);
                    found = true;
                    break;
                }
                else if (known_param->type == param_natural_number)
                {
                    int val = atoi(input_param->value);
                    if (val >= 0)
                    {
                        *((unsigned int*) known_param->var) = (unsigned int) val;
                        found = true;
                        break;
                    }
                    else
                    {
                        MXS_ERROR("%s: Parameter '%s' must not be negative", myName, known_param->name);
                        return false;
                    }
                }
            }
        }
        if (known_param->mandatory && !found)
        {
            MXS_ERROR("%s: Mandatory parameter missing: '%s'", myName, known_param->name);
            return false;
        }
    }

    return true;
}

/**
 * Writes date/time corresponding unix timestamp 'ts' to 'buffer'
 *
 * @param ts             unix timestamp
 * @param buffer         output string buffer
 * @param bufferSize     size of buffer in bytes
 *
*/
static void
getTimestampAsDateTime(unsigned int ts, char* buffer, const unsigned int bufferSize)
{
    time_t t = ts;
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(buffer, bufferSize, "%F %T", &tm);
}

/**
 * Writes the statistics/counter log, if 'loggingInterval' (seconds)
 * have elapsed since the last logging.
 *
 * @param session          Session context
 * @param loggingInterval  How often the log is written (seconds)
 *
*/
static void
writeLogIfNeeded(LS_SESSION* session, unsigned long loggingInterval)
{
    unsigned long now = (unsigned long) time(NULL);

    /* log if the interval has passed or the clock has jumped */
    bool should_log = now >= session->timestamp + loggingInterval || session->timestamp > now;
    if (!should_log)
    {
        return;
    }

    char start_str[1024], now_str[1024];
    getTimestampAsDateTime(session->timestamp, start_str, sizeof(start_str));
    getTimestampAsDateTime(now, now_str, sizeof(now_str));

    fprintf(session->fp, "%s,%s,%u,%u,%u,%u\n",
        start_str,
        now_str,
        session->counters.selectQuery,
        session->counters.insertQuery,
        session->counters.updateQuery,
        session->counters.deleteQuery);
    fflush(session->fp);

    resetCounters(session);
    session->timestamp = now;
}

/**
 * Passes the query downstream
 * 
 * @param session   Session context
 * @param queue     Query data
 * 
*/
static int
passQueryDownstream(LS_SESSION* session, GWBUF* queue)
{
    return session->down.routeQuery(session->down.instance, session->down.session, queue);
}

/**
 * Updates statistics/counters based on 'queryStr'
 * 
 * @param session   Session context
 * @param queryStr  Query as string
 * 
*/
static void
updateCounters(LS_SESSION* session, const char* queryStr)
{
    CounterBinding counterBind;
    const unsigned int queryTypeMax = sizeof(session->counterBind) / sizeof(session->counterBind[0]);

    for (unsigned int counter_idx = 0; counter_idx < queryTypeMax; ++counter_idx)
    {
        counterBind = session->counterBind[counter_idx];
        if (strncasecmp(counterBind.queryType, queryStr, strlen(counterBind.queryType)) == 0)
        {
            ++(*counterBind.counter);
            return; /* query string can be only of one type */
        }
    }
}

/**
 * Resets statistics/counters to zero
 * 
 * @param session   Session context
 * 
*/
static void
resetCounters(LS_SESSION* session)
{
    memset(&session->counters, 0, sizeof(session->counters));
}

/**
 * Bind counter types to variables
 *
 * @param session   Session context
 *
*/
static void
bindCounters(LS_SESSION* session)
{
    session->counterBind[0] = (CounterBinding) { .counter = &session->counters.selectQuery, .queryType = "select" };
    session->counterBind[1] = (CounterBinding) { .counter = &session->counters.insertQuery, .queryType = "insert" };
    session->counterBind[2] = (CounterBinding) { .counter = &session->counters.updateQuery, .queryType = "update" };
    session->counterBind[3] = (CounterBinding) { .counter = &session->counters.deleteQuery, .queryType = "delete" };
}
