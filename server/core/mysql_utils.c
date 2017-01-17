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
 * @file mysql_utils.c  - Binary MySQL data processing utilities
 *
 * This file contains functions that are used when processing binary format
 * information. The MySQL protocol uses the binary format in result sets and
 * row based replication.
 */

#include <maxscale/mysql_utils.h>
#include <string.h>
#include <stdbool.h>
#include <maxscale/alloc.h>
#include <maxscale/log_manager.h>
#include <maxscale/debug.h>
#include <maxscale/config.h>

/**
 * @brief Calculate the length of a length-encoded integer in bytes
 *
 * @param ptr Start of the length encoded value
 * @return Number of bytes before the actual value
 */
size_t mxs_leint_bytes(const uint8_t* ptr)
{
    uint8_t val = *ptr;
    if (val < 0xfb)
    {
        return 1;
    }
    else if (val == 0xfc)
    {
        return 3;
    }
    else if (val == 0xfd)
    {
        return 4;
    }
    else
    {
        return 9;
    }
}

/**
 * @brief Converts a length-encoded integer to @c uint64_t
 *
 * @see https://dev.mysql.com/doc/internals/en/integer.html
 * @param c Pointer to the first byte of a length-encoded integer
 * @return The value converted to a standard unsigned integer
 */
uint64_t mxs_leint_value(const uint8_t* c)
{
    uint64_t sz = 0;

    if (*c < 0xfb)
    {
        sz = *c;
    }
    else if (*c == 0xfc)
    {
        memcpy(&sz, c + 1, 2);
    }
    else if (*c == 0xfd)
    {
        memcpy(&sz, c + 1, 3);
    }
    else if (*c == 0xfe)
    {
        memcpy(&sz, c + 1, 8);
    }
    else
    {
        ss_dassert(*c == 0xff);
        MXS_ERROR("Unexpected length encoding '%x' encountered when reading "
                  "length-encoded integer.", *c);
    }

    return sz;
}

/**
 * Converts a length-encoded integer into a standard unsigned integer
 * and advances the pointer to the next unrelated byte.
 *
 * @param c Pointer to the first byte of a length-encoded integer
 */
uint64_t mxs_leint_consume(uint8_t ** c)
{
    uint64_t rval = mxs_leint_value(*c);
    *c += mxs_leint_bytes(*c);
    return rval;
}

/**
 * @brief Consume and duplicate a length-encoded string
 *
 * Converts a length-encoded string to a C string and advances the pointer to
 * the first byte after the string. The caller is responsible for freeing
 * the returned string.
 * @param c Pointer to the first byte of a valid packet.
 * @return The newly allocated string or NULL if memory allocation failed
 */
char* mxs_lestr_consume_dup(uint8_t** c)
{
    uint64_t slen = mxs_leint_consume(c);
    char *str = MXS_MALLOC((slen + 1) * sizeof(char));

    if (str)
    {
        memcpy(str, *c, slen);
        str[slen] = '\0';
        *c += slen;
    }

    return str;
}

/**
 * @brief Consume a length-encoded string
 *
 * Converts length-encoded strings to character strings and advanced
 * the pointer to the next unrelated byte.
 * @param c Pointer to the start of the length-encoded string
 * @param size Pointer to a variable where the size of the string is stored
 * @return Pointer to the start of the string
 */
char* mxs_lestr_consume(uint8_t** c, size_t *size)
{
    uint64_t slen = mxs_leint_consume(c);
    *size = slen;
    char* start = (char*) *c;
    *c += slen;
    return start;
}



/**
 * Creates a connection to a MySQL database engine. If necessary, initializes SSL.
 *
 * @param con    A valid MYSQL structure.
 * @param server The server on which the MySQL engine is running.
 * @param user   The MySQL login ID.
 * @param passwd The password for the user.
 */
MYSQL *mxs_mysql_real_connect(MYSQL *con, SERVER *server, const char *user, const char *passwd)
{
    SSL_LISTENER *listener = server->server_ssl;

    if (listener)
    {
        mysql_ssl_set(con, listener->ssl_key, listener->ssl_cert, listener->ssl_ca_cert, NULL, NULL);
    }

    return mysql_real_connect(con, server->name, user, passwd, NULL, server->port, NULL, 0);
}

bool mxs_mysql_trim_quotes(char *s)
{
    bool dequoted = true;

    char *i = s;
    char *end = s + strlen(s);

    // Remove space from the beginning
    while (*i && isspace(*i))
    {
        ++i;
    }

    if (*i)
    {
        // Remove space from the end
        while (isspace(*(end - 1)))
        {
            *(end - 1) = 0;
            --end;
        }

        ss_dassert(end > i);

        char quote;

        switch (*i)
        {
        case '\'':
        case '"':
        case '`':
            quote = *i;
            ++i;
            break;

        default:
            quote = 0;
        }

        if (quote)
        {
            --end;

            if (*end == quote)
            {
                *end = 0;

                memmove(s, i, end - i + 1);
            }
            else
            {
                dequoted = false;
            }
        }
        else if (i != s)
        {
            memmove(s, i, end - i + 1);
        }
    }
    else
    {
        *s = 0;
    }

    return dequoted;
}


mxs_mysql_name_kind_t mxs_mysql_name_to_pcre(char *pcre,
                                             const char *mysql,
                                             mxs_pcre_quote_approach_t approach)
{
    mxs_mysql_name_kind_t rv = MXS_MYSQL_NAME_WITHOUT_WILDCARD;

    while (*mysql)
    {
        switch (*mysql)
        {
        case '%':
            if (approach == MXS_PCRE_QUOTE_WILDCARD)
            {
                *pcre = '.';
                pcre++;
                *pcre = '*';
            }
            rv = MXS_MYSQL_NAME_WITH_WILDCARD;
            break;

        case '\'':
        case '^':
        case '.':
        case '$':
        case '|':
        case '(':
        case ')':
        case '[':
        case ']':
        case '*':
        case '+':
        case '?':
        case '{':
        case '}':
            *pcre++ = '\\';
        // Flowthrough
        default:
            *pcre = *mysql;
        }

        ++pcre;
        ++mysql;
    }

    *pcre = 0;

    return rv;
}
