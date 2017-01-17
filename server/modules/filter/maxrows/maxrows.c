/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl.
 *
 * Change Date: 2019-07-01
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file maxrows.c - Result set limit Filter
 * @verbatim
 *
 *
 * The filter returns a void result set if the number of rows in the result set
 * from backend exceeds the max_rows parameter.
 *
 * Date         Who                   Description
 * 26/10/2016   Massimiliano Pinto    Initial implementation
 * 04/11/2016   Massimiliano Pinto    Addition of SERVER_MORE_RESULTS_EXIST flag (0x0008)
 *                                    detection in handle_expecting_rows().
 * 07/11/2016   Massimiliano Pinto    handle_expecting_rows renamed to handle_rows
 * 20/12/2016   Massimiliano Pinto    csdata->res.n_rows counter works with MULTI_RESULT
 *                                    and large packets (> 16MB)
 *
 * @endverbatim
 */

#define MXS_MODULE_NAME "maxrows"
#include <maxscale/alloc.h>
#include <maxscale/filter.h>
#include <maxscale/gwdirs.h>
#include <maxscale/log_manager.h>
#include <maxscale/modinfo.h>
#include <maxscale/modutil.h>
#include <maxscale/mysql_utils.h>
#include <maxscale/query_classifier.h>
#include <stdbool.h>
#include <stdint.h>
#include <maxscale/buffer.h>
#include <maxscale/protocol/mysql.h>
#include <maxscale/debug.h>
#include "maxrows.h"

static MXS_FILTER *createInstance(const char *name, char **options, CONFIG_PARAMETER *);
static MXS_FILTER_SESSION *newSession(MXS_FILTER *instance, SESSION *session);
static void    closeSession(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata);
static void    freeSession(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata);
static void    setDownstream(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, MXS_DOWNSTREAM *downstream);
static void    setUpstream(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, MXS_UPSTREAM *upstream);
static int     routeQuery(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, GWBUF *queue);
static int     clientReply(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, GWBUF *queue);
static void    diagnostics(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, DCB *dcb);
static uint64_t getCapabilities(void);

/* Global symbols of the Module */

/**
 * The module entry point function, called when the module is loaded.
 *
 * @return The module object.
 */
MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_FILTER_OBJECT object =
    {
        createInstance,
        newSession,
        closeSession,
        freeSession,
        setDownstream,
        setUpstream,
        routeQuery,
        clientReply,
        diagnostics,
        getCapabilities,
        NULL, // No destroyInstance
    };

    static MXS_MODULE info =
    {
        MXS_MODULE_API_FILTER,
        MXS_MODULE_IN_DEVELOPMENT,
        MXS_FILTER_VERSION,
        "A filter that is capable of limiting the resultset number of rows.",
        "V1.0.0",
        &object,
        NULL, /* Process init. */
        NULL, /* Process finish. */
        NULL, /* Thread init. */
        NULL, /* Thread finish. */
        {
            {
                "max_resultset_rows",
                MXS_MODULE_PARAM_COUNT,
                MAXROWS_DEFAULT_MAX_RESULTSET_ROWS
            },
            {
                "max_resultset_size",
                MXS_MODULE_PARAM_COUNT,
                MAXROWS_DEFAULT_MAX_RESULTSET_SIZE
            },
            {
                "debug",
                MXS_MODULE_PARAM_COUNT,
                MAXROWS_DEFAULT_DEBUG
            },
            {MXS_END_MODULE_PARAMS}
        }
    };

    return &info;
};

/* Implementation */

typedef struct maxrows_config
{
    uint32_t    max_resultset_rows;
    uint32_t    max_resultset_size;
    uint32_t    debug;
} MAXROWS_CONFIG;

typedef struct maxrows_instance
{
    const char            *name;
    MAXROWS_CONFIG         config;
} MAXROWS_INSTANCE;

typedef enum maxrows_session_state
{
    MAXROWS_EXPECTING_RESPONSE = 1, // A select has been sent, and we are waiting for the response.
    MAXROWS_EXPECTING_FIELDS,       // A select has been sent, and we want more fields.
    MAXROWS_EXPECTING_ROWS,         // A select has been sent, and we want more rows.
    MAXROWS_EXPECTING_NOTHING,      // We are not expecting anything from the server.
    MAXROWS_IGNORING_RESPONSE,      // We are not interested in the data received from the server.
} maxrows_session_state_t;

typedef struct maxrows_response_state
{
    GWBUF* data;          /**< Response data, possibly incomplete. */
    size_t n_totalfields; /**< The number of fields a resultset contains. */
    size_t n_fields;      /**< How many fields we have received, <= n_totalfields. */
    size_t n_rows;        /**< How many rows we have received. */
    size_t offset;        /**< Where we are in the response buffer. */
} MAXROWS_RESPONSE_STATE;

static void maxrows_response_state_reset(MAXROWS_RESPONSE_STATE *state);

typedef struct maxrows_session_data
{
    MAXROWS_INSTANCE       *instance;          /**< The maxrows instance the session is associated with. */
    MXS_DOWNSTREAM          down;              /**< The previous filter or equivalent. */
    MXS_UPSTREAM            up;                /**< The next filter or equivalent. */
    MAXROWS_RESPONSE_STATE  res;               /**< The response state. */
    SESSION                *session;           /**< The session this data is associated with. */
    maxrows_session_state_t state;
    bool                    large_packet;      /**< Large packet (> 16MB)) indicator */
    bool                    discard_resultset; /**< Discard resultset indicator */
} MAXROWS_SESSION_DATA;

static MAXROWS_SESSION_DATA *maxrows_session_data_create(MAXROWS_INSTANCE *instance, SESSION *session);
static void maxrows_session_data_free(MAXROWS_SESSION_DATA *data);

static int handle_expecting_fields(MAXROWS_SESSION_DATA *csdata);
static int handle_expecting_nothing(MAXROWS_SESSION_DATA *csdata);
static int handle_expecting_response(MAXROWS_SESSION_DATA *csdata);
static int handle_rows(MAXROWS_SESSION_DATA *csdata);
static int handle_ignoring_response(MAXROWS_SESSION_DATA *csdata);
static bool process_params(char **options, CONFIG_PARAMETER *params, MAXROWS_CONFIG* config);

static int send_upstream(MAXROWS_SESSION_DATA *csdata);
static int send_ok_upstream(MAXROWS_SESSION_DATA *csdata);

/* API BEGIN */

/**
 * Create an instance of the maxrows filter for a particular service
 * within MaxScale.
 *
 * @param name     The name of the instance (as defined in the config file).
 * @param options  The options for this filter
 * @param params   The array of name/value pair parameters for the filter
 *
 * @return The instance data for this new instance
 */
static MXS_FILTER *createInstance(const char *name, char **options, CONFIG_PARAMETER *params)
{
    MAXROWS_INSTANCE *cinstance = MXS_CALLOC(1, sizeof(MAXROWS_INSTANCE));

    if (cinstance)
    {
        cinstance->name = name;
        cinstance->config.max_resultset_rows = config_get_integer(params, "max_resultset_rows");
        cinstance->config.max_resultset_size = config_get_integer(params, "max_resultset_size");
        cinstance->config.debug = config_get_integer(params, "debug");
    }

    return (MXS_FILTER*)cinstance;
}

/**
 * Associate a new session with this instance of the filter.
 *
 * @param instance  The maxrows instance data
 * @param session   The session itself
 *
 * @return Session specific data for this session
 */
static MXS_FILTER_SESSION *newSession(MXS_FILTER *instance, SESSION *session)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = maxrows_session_data_create(cinstance, session);

    return (MXS_FILTER_SESSION*)csdata;
}

/**
 * A session has been closed.
 *
 * @param instance  The maxrows instance data
 * @param sdata     The session data of the session being closed
 */
static void closeSession(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;
}

/**
 * Free the session data.
 *
 * @param instance  The maxrows instance data
 * @param sdata     The session data of the session being closed
 */
static void freeSession(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    maxrows_session_data_free(csdata);
}

/**
 * Set the downstream component for this filter.
 *
 * @param instance    The maxrowsinstance data
 * @param sdata       The session data of the session
 * @param down        The downstream filter or router
 */
static void setDownstream(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, MXS_DOWNSTREAM *down)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    csdata->down = *down;
}

/**
 * Set the upstream component for this filter.
 *
 * @param instance    The maxrows instance data
 * @param sdata       The session data of the session
 * @param up          The upstream filter or router
 */
static void setUpstream(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, MXS_UPSTREAM *up)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    csdata->up = *up;
}

/**
 * A request on its way to a backend is delivered to this function.
 *
 * @param instance  The filter instance data
 * @param sdata     The filter session data
 * @param buffer    Buffer containing an MySQL protocol packet.
 */
static int routeQuery(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, GWBUF *packet)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    uint8_t *data = GWBUF_DATA(packet);

    // All of these should be guaranteed by RCAP_TYPE_TRANSACTION_TRACKING
    ss_dassert(GWBUF_IS_CONTIGUOUS(packet));
    ss_dassert(GWBUF_LENGTH(packet) >= MYSQL_HEADER_LEN + 1);
    ss_dassert(MYSQL_GET_PAYLOAD_LEN(data) + MYSQL_HEADER_LEN == GWBUF_LENGTH(packet));

    maxrows_response_state_reset(&csdata->res);
    csdata->state = MAXROWS_IGNORING_RESPONSE;
    csdata->large_packet = false;
    csdata->discard_resultset = false;

    switch ((int)MYSQL_GET_COMMAND(data))
    {
    case MYSQL_COM_QUERY:
    case MYSQL_COM_STMT_EXECUTE:
        {
            csdata->state = MAXROWS_EXPECTING_RESPONSE;
            break;
        }

    default:
        break;
    }

    if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
    {
        MXS_NOTICE("Maxrows filter is sending data.");
    }

    return csdata->down.routeQuery(csdata->down.instance, csdata->down.session, packet);
}

/**
 * A response on its way to the client is delivered to this function.
 *
 * @param instance  The filter instance data
 * @param sdata     The filter session data
 * @param queue     The query data
 */
static int clientReply(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, GWBUF *data)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    int rv;

    if (csdata->res.data)
    {
        gwbuf_append(csdata->res.data, data);
    }
    else
    {
        csdata->res.data = data;
    }

    if (csdata->state != MAXROWS_IGNORING_RESPONSE)
    {
        if (!csdata->discard_resultset)
        {
            if (gwbuf_length(csdata->res.data) > csdata->instance->config.max_resultset_size)
            {
                if (csdata->instance->config.debug & MAXROWS_DEBUG_DISCARDING)
                {
                    MXS_NOTICE("Current size %uB of resultset, at least as much "
                               "as maximum allowed size %uKiB. Not returning data.",
                               gwbuf_length(csdata->res.data),
                               csdata->instance->config.max_resultset_size / 1024);
                }

                csdata->discard_resultset = true;
            }
        }
    }

    switch (csdata->state)
    {
    case MAXROWS_EXPECTING_FIELDS:
        rv = handle_expecting_fields(csdata);
        break;

    case MAXROWS_EXPECTING_NOTHING:
        rv = handle_expecting_nothing(csdata);
        break;

    case MAXROWS_EXPECTING_RESPONSE:
        rv = handle_expecting_response(csdata);
        break;

    case MAXROWS_EXPECTING_ROWS:
        rv = handle_rows(csdata);
        break;

    case MAXROWS_IGNORING_RESPONSE:
        rv = handle_ignoring_response(csdata);
        break;

    default:
        MXS_ERROR("Internal filter logic broken, unexpected state: %d", csdata->state);
        ss_dassert(!true);
        rv = send_upstream(csdata);
        maxrows_response_state_reset(&csdata->res);
        csdata->state = MAXROWS_IGNORING_RESPONSE;
    }

    return rv;
}

/**
 * Diagnostics routine
 *
 * If csdata is NULL then print diagnostics on the instance as a whole,
 * otherwise print diagnostics for the particular session.
 *
 * @param instance  The filter instance
 * @param fsession  Filter session, may be NULL
 * @param dcb       The DCB for diagnostic output
 */
static void diagnostics(MXS_FILTER *instance, MXS_FILTER_SESSION *sdata, DCB *dcb)
{
    MAXROWS_INSTANCE *cinstance = (MAXROWS_INSTANCE*)instance;
    MAXROWS_SESSION_DATA *csdata = (MAXROWS_SESSION_DATA*)sdata;

    dcb_printf(dcb, "Maxrows filter is working\n");
}


/**
 * Capability routine.
 *
 * @return The capabilities of the filter.
 */
static uint64_t getCapabilities(void)
{
    return RCAP_TYPE_STMT_INPUT | RCAP_TYPE_STMT_OUTPUT;
}

/* API END */

/**
 * Reset maxrows response state
 *
 * @param state Pointer to object.
 */
static void maxrows_response_state_reset(MAXROWS_RESPONSE_STATE *state)
{
    state->data = NULL;
    state->n_totalfields = 0;
    state->n_fields = 0;
    state->n_rows = 0;
    state->offset = 0;
}

/**
 * Create maxrows session data
 *
 * @param instance The maxrows instance this data is associated with.
 *
 * @return Session data or NULL if creation fails.
 */
static MAXROWS_SESSION_DATA *maxrows_session_data_create(MAXROWS_INSTANCE *instance,
                                                         SESSION* session)
{
    MAXROWS_SESSION_DATA *data = (MAXROWS_SESSION_DATA*)MXS_CALLOC(1, sizeof(MAXROWS_SESSION_DATA));

    if (data)
    {
        ss_dassert(session->client_dcb);
        ss_dassert(session->client_dcb->data);

        MYSQL_session *mysql_session = (MYSQL_session*)session->client_dcb->data;
        data->instance = instance;
        data->session = session;
        data->state = MAXROWS_EXPECTING_NOTHING;
    }

    return data;
}

/**
 * Free maxrows session data.
 *
 * @param A maxrows session data previously allocated using session_data_create().
 */
static void maxrows_session_data_free(MAXROWS_SESSION_DATA* data)
{
    if (data)
    {
        MXS_FREE(data);
    }
}

/**
 * Called when resultset field information is handled.
 *
 * @param csdata The maxrows session data.
 */
static int handle_expecting_fields(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->state == MAXROWS_EXPECTING_FIELDS);
    ss_dassert(csdata->res.data);

    int rv = 1;

    bool insufficient = false;

    size_t buflen = gwbuf_length(csdata->res.data);

    while (!insufficient && (buflen - csdata->res.offset >= MYSQL_HEADER_LEN))
    {
        uint8_t header[MYSQL_HEADER_LEN + 1];
        gwbuf_copy_data(csdata->res.data, csdata->res.offset, MYSQL_HEADER_LEN + 1, header);

        size_t packetlen = MYSQL_HEADER_LEN + MYSQL_GET_PAYLOAD_LEN(header);

        if (csdata->res.offset + packetlen <= buflen)
        {
            // We have at least one complete packet.
            int command = (int)MYSQL_GET_COMMAND(header);

            switch (command)
            {
            case 0xfe: // EOF, the one after the fields.
                csdata->res.offset += packetlen;
                csdata->state = MAXROWS_EXPECTING_ROWS;
                rv = handle_rows(csdata);
                break;

            default: // Field information.
                csdata->res.offset += packetlen;
                ++csdata->res.n_fields;
                ss_dassert(csdata->res.n_fields <= csdata->res.n_totalfields);
                break;
            }
        }
        else
        {
            // We need more data
            insufficient = true;
        }
    }

    return rv;
}

/**
 * Called when data is received (even if nothing is expected) from the server.
 *
 * @param csdata The maxrows session data.
 */
static int handle_expecting_nothing(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->state == MAXROWS_EXPECTING_NOTHING);
    ss_dassert(csdata->res.data);
    MXS_ERROR("Received data from the backend although we were expecting nothing.");
    ss_dassert(!true);

    return send_upstream(csdata);
}

/**
 * Called when a response is received from the server.
 *
 * @param csdata The maxrows session data.
 */
static int handle_expecting_response(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->state == MAXROWS_EXPECTING_RESPONSE);
    ss_dassert(csdata->res.data);

    int rv = 1;
    size_t buflen = gwbuf_length(csdata->res.data);

    // Reset field counters
    csdata->res.n_fields = 0;
    csdata->res.n_totalfields = 0;
    // Reset large packet var
    csdata->large_packet = false;

    if (buflen >= MYSQL_HEADER_LEN + 1) // We need the command byte.
    {
        // Reserve enough space to accomodate for the largest length encoded integer,
        // which is type field + 8 bytes.
        uint8_t header[MYSQL_HEADER_LEN + 1 + 8];

        // Read packet header from buffer at current offset
        gwbuf_copy_data(csdata->res.data, csdata->res.offset, MYSQL_HEADER_LEN + 1, header);

        switch ((int)MYSQL_GET_COMMAND(header))
        {
        case 0x00: // OK
        case 0xff: // ERR
            /**
             * This also handles the OK packet that terminates
             * a Multi-Resultset seen in handle_rows()
             */
            if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
            {
                if (csdata->res.n_rows)
                {
                    MXS_NOTICE("OK or ERR seen. The resultset has %lu rows.%s",
                               csdata->res.n_rows,
                               csdata->discard_resultset ? " [Discarded]" : "");
                }
                else
                {
                    MXS_NOTICE("OK or ERR");
                }
            }

            if (csdata->discard_resultset)
            {
                rv = send_ok_upstream(csdata);
                csdata->state = MAXROWS_EXPECTING_NOTHING;
            }
            else
            {
                rv = send_upstream(csdata);
                csdata->state = MAXROWS_IGNORING_RESPONSE;
            }
            break;

        case 0xfb: // GET_MORE_CLIENT_DATA/SEND_MORE_CLIENT_DATA
            if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
            {
                MXS_NOTICE("GET_MORE_CLIENT_DATA");
            }
            rv = send_upstream(csdata);
            csdata->state = MAXROWS_IGNORING_RESPONSE;
            break;

        default:
            if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
            {
                MXS_NOTICE("RESULTSET");
            }

            if (csdata->res.n_totalfields != 0)
            {
                // We've seen the header and have figured out how many fields there are.
                csdata->state = MAXROWS_EXPECTING_FIELDS;
                rv = handle_expecting_fields(csdata);
            }
            else
            {
                // mxs_leint_bytes() returns the length of the int type field + the size of the
                // integer.
                size_t n_bytes = mxs_leint_bytes(&header[4]);

                if (MYSQL_HEADER_LEN + n_bytes <= buflen)
                {
                    // Now we can figure out how many fields there are, but first we
                    // need to copy some more data.
                    gwbuf_copy_data(csdata->res.data,
                                    MYSQL_HEADER_LEN + 1, n_bytes - 1, &header[MYSQL_HEADER_LEN + 1]);

                    csdata->res.n_totalfields = mxs_leint_value(&header[4]);
                    csdata->res.offset += MYSQL_HEADER_LEN + n_bytes;

                    csdata->state = MAXROWS_EXPECTING_FIELDS;
                    rv = handle_expecting_fields(csdata);
                }
                else
                {
                    // We need more data. We will be called again, when data is available.
                }
            }
            break;
        }
    }

    return rv;
}

/**
 * Called when resultset rows are handled.
 *
 * @param csdata The maxrows session data.
 */
static int handle_rows(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->state == MAXROWS_EXPECTING_ROWS);
    ss_dassert(csdata->res.data);

    int rv = 1;
    bool insufficient = false;
    size_t buflen = gwbuf_length(csdata->res.data);

    while (!insufficient && (buflen - csdata->res.offset >= MYSQL_HEADER_LEN))
    {
        bool pending_large_data = csdata->large_packet;
        // header array holds a full EOF packet
        uint8_t header[MAXROWS_EOF_PACKET_LEN];
        gwbuf_copy_data(csdata->res.data, csdata->res.offset, MAXROWS_EOF_PACKET_LEN, header);

        size_t packetlen = MYSQL_HEADER_LEN + MYSQL_GET_PAYLOAD_LEN(header);

        if (csdata->res.offset + packetlen <= buflen)
        {
            /* Check for large packet packet terminator:
             * min is 4 bytes "0x0 0x0 0x0 0xseq_no and
             * max is 1 byte less than EOF_PACKET_LEN
             * If true skip data processing.
             */
            if (pending_large_data && (packetlen >= MYSQL_HEADER_LEN && packetlen < MAXROWS_EOF_PACKET_LEN))
            {
                // Update offset, number of rows and break
                csdata->res.offset += packetlen;
                csdata->res.n_rows++;

                ss_dassert(csdata->res.offset == buflen);
                break;
            }

            /*
             * Check packet size against MYSQL_PACKET_LENGTH_MAX
             * If true then break as received could be not complete
             * EOF or OK packet could be seen after receiving the full large packet
             */
            if (packetlen == (MYSQL_PACKET_LENGTH_MAX + MYSQL_HEADER_LEN))
            {
                // Mark the beginning of a large packet receiving
                csdata->large_packet = true;
                // Just update offset and break
                csdata->res.offset += packetlen;

                ss_dassert(csdata->res.offset == buflen);
                break;
            }
            else
            {
                // Reset large packet indicator
                csdata->large_packet = false;
            }

            // We have at least one complete packet and we can process the command byte.
            int command = (int)MYSQL_GET_COMMAND(header);

            switch (command)
            {
            case 0xff: // ERR packet after the rows.
                csdata->res.offset += packetlen;
                ss_dassert(csdata->res.offset == buflen);

                // This is the end of resultset: set big packet var to false
                csdata->large_packet = false;

                if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
                {
                    MXS_NOTICE("Error packet seen while handling result set");
                }

                /*
                 * This is the ERR packet that could terminate a Multi-Resultset.
                 */

                // Send data in buffer or empty resultset
                if (csdata->discard_resultset)
                {
                    rv = send_ok_upstream(csdata);
                }
                else
                {
                    rv = send_upstream(csdata);
                }

                csdata->state = MAXROWS_EXPECTING_NOTHING;

                break;

            /* OK could the last packet in the Multi-Resultset transmission:
             * this is handled by handle_expecting_response()
             *
             * It could also be sent instead of EOF from as in MySQL 5.7.5
             * if client sends CLIENT_DEPRECATE_EOF capability OK packet could
             * have the SERVER_MORE_RESULTS_EXIST flag.
             * Flags in the OK packet are at the same offset as in EOF.
             *
             * NOTE: not supported right now
             */
            case 0xfe: // EOF, the one after the rows.
                csdata->res.offset += packetlen;
                ss_dassert(csdata->res.offset == buflen);

                /* EOF could be the last packet in the transmission:
                 * check first whether SERVER_MORE_RESULTS_EXIST flag is set.
                 * If so more results set could come. The end of stream
                 * will be an OK packet.
                 */
                if (packetlen < MAXROWS_EOF_PACKET_LEN)
                {
                    MXS_ERROR("EOF packet has size of %lu instead of %d", packetlen, MAXROWS_EOF_PACKET_LEN);
                    rv = send_ok_upstream(csdata);
                    csdata->state = MAXROWS_EXPECTING_NOTHING;
                    break;
                }

                int flags = gw_mysql_get_byte2(header + MAXROWS_MYSQL_EOF_PACKET_FLAGS_OFFSET);

                // Check whether the EOF terminates the resultset or indicates MORE_RESULTS
                if (!(flags & SERVER_MORE_RESULTS_EXIST))
                {
                    // End of the resultset
                    if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
                    {
                        MXS_NOTICE("OK or EOF packet seen: the resultset has %lu rows.%s",
                                   csdata->res.n_rows,
                                   csdata->discard_resultset ? " [Discarded]" : "");
                    }

                    // Discard data or send data
                    if (csdata->discard_resultset)
                    {
                        rv = send_ok_upstream(csdata);
                    }
                    else
                    {
                        rv = send_upstream(csdata);
                    }

                    csdata->state = MAXROWS_EXPECTING_NOTHING;
                }
                else
                {
                    /*
                     * SERVER_MORE_RESULTS_EXIST flag is present: additional resultsets will come.
                     *
                     * Note: the OK packet that terminates the Multi-Resultset
                     * is handled by handle_expecting_response()
                     */

                    csdata->state = MAXROWS_EXPECTING_RESPONSE;

                    if (csdata->instance->config.debug & MAXROWS_DEBUG_DECISIONS)
                    {
                        MXS_NOTICE("EOF or OK packet seen with SERVER_MORE_RESULTS_EXIST flag:"
                                   " waiting for more data (%lu rows so far)",
                                   csdata->res.n_rows);
                    }
                }

                break;

            case 0xfb: // NULL
            default: // length-encoded-string
                csdata->res.offset += packetlen;
                // Increase res.n_rows counter while not receiving large packets
                if (!csdata->large_packet)
                {
                    csdata->res.n_rows++;
                }

                // Check for max_resultset_rows limit
                if (!csdata->discard_resultset)
                {
                    if (csdata->res.n_rows > csdata->instance->config.max_resultset_rows)
                    {
                        if (csdata->instance->config.debug & MAXROWS_DEBUG_DISCARDING)
                        {
                            MXS_INFO("max_resultset_rows %lu reached, not returning the resultset.", csdata->res.n_rows);
                        }

                        // Set the discard indicator
                        csdata->discard_resultset = true;
                    }
                }
                break;
            }
        }
        else
        {
            // We need more data
            insufficient = true;
        }
    }

    return rv;
}

/**
 * Called when all data from the server is ignored.
 *
 * @param csdata The maxrows session data.
 */
static int handle_ignoring_response(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->state == MAXROWS_IGNORING_RESPONSE);
    ss_dassert(csdata->res.data);

    return send_upstream(csdata);
}

/**
 * Send data upstream.
 *
 * @param csdata Session data
 *
 * @return Whatever the upstream returns.
 */
static int send_upstream(MAXROWS_SESSION_DATA *csdata)
{
    ss_dassert(csdata->res.data != NULL);

    int rv = csdata->up.clientReply(csdata->up.instance, csdata->up.session, csdata->res.data);
    csdata->res.data = NULL;

    return rv;
}

/**
 * Send OK packet data upstream.
 *
 * @param csdata Session data
 *
 * @return Whatever the upstream returns.
 */
static int send_ok_upstream(MAXROWS_SESSION_DATA *csdata)
{
    /* Note: sequence id is always 01 (4th byte) */
    uint8_t ok[MAXROWS_OK_PACKET_LEN] = {07, 00, 00, 01, 00, 00, 00, 02, 00, 00, 00};
    GWBUF *packet = gwbuf_alloc(MAXROWS_OK_PACKET_LEN);
    uint8_t *ptr = GWBUF_DATA(packet);
    memcpy(ptr, &ok, MAXROWS_OK_PACKET_LEN);

    ss_dassert(csdata->res.data != NULL);

    int rv = csdata->up.clientReply(csdata->up.instance, csdata->up.session, packet);
    gwbuf_free(csdata->res.data);
    csdata->res.data = NULL;

    return rv;
}
