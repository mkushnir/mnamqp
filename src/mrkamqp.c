#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>

#include <mrkcommon/bytestream.h>
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkthr.h>
#include <mrkamqp.h>

#include "diag.h"


amqp_conn_t *
amqp_conn_new(char *host,
              int port,
              char *user,
              char *password,
              char *vhost,
              int heartbeat,
              int frame_max)
{
    amqp_conn_t *res;

    if ((res = malloc(sizeof(amqp_conn_t))) == NULL) {
        FAIL("malloc");
    }

    res->host = strdup(host);
    res->port = port;
    res->user = strdup(user);
    res->password = strdup(password);
    res->vhost = strdup(vhost);
    res->heartbeat = heartbeat;
    res->frame_max = frame_max;
    return res;
}


void
amqp_conn_destroy(amqp_conn_t **conn)
{
    if ((*conn) != NULL) {
        if ((*conn)->host != NULL) {
            free((*conn)->host);
        }
        if ((*conn)->user != NULL) {
            free((*conn)->user);
        }
        if ((*conn)->password != NULL) {
            free((*conn)->password);
        }
        if ((*conn)->vhost != NULL) {
            free((*conn)->vhost);
        }
        free(*conn);
        *conn = NULL;
    }
}


int
amqp_conn_open(amqp_conn_t *conn)
{
    struct addrinfo hints, *ainfos, *ai;
    char portstr[32];

    snprintf(portstr, sizeof(portstr), "%d", conn->port);
    memset(&hints, 0, sizeof(hints));
    //hints.ai_family = PF_INET;
    hints.ai_socktype = SOCK_STREAM;
    //hints.ai_protocol = IPPROTO_TCP;

    ainfos = NULL;
    if (getaddrinfo(conn->host, portstr, &hints, &ainfos) != 0) {
        TRRET(AMQP_CONN_OPEN + 1);
    }

    for (ai = ainfos; ai != NULL; ai = ai->ai_next) {
        if ((conn->fd = socket(ai->ai_family,
                               ai->ai_socktype,
                               ai->ai_protocol)) < 0) {
            continue;
        }

        if (bind(conn->fd, ai->ai_addr, ai->ai_addrlen) != 0) {
            TRRET(AMQP_CONN_OPEN + 1);
        }
        break;
    }

    freeaddrinfo(ainfos);

    if (conn->fd < 0) {
        TRRET(AMQP_CONN_OPEN + 1);
    }

    return 0;
}


void
amqp_conn_close(amqp_conn_t *conn)
{
    //notify_channels_of_close()
    // >>> connection_close
    // <<< connection_close_ok
    close(conn->fd);
}


void
amqp_conn_run(UNUSED amqp_conn_t *conn)
{
    // >>> AMQP0091
    // <<< connection_start
    // >>> connection_start_ok
    // <<< connection_tune
    // >>> connection_tune_ok
    // >>> connection_open
    // <<< connection_open_ok
}


amqp_channel_t *
amqp_channel_new(UNUSED amqp_conn_t *conn, UNUSED int confirm_mode)
{
    amqp_channel_t *res;
    // >>> channel_open
    // <<< channel_open_ok
    res = NULL;

    return res;
}
