#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>
#include <mrkcommon/traversedir.h>

#include <mrkthr.h>

#include <mrkamqp_private.h>


#include "diag.h"

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

static char *user = "guest";
static char *password = "guest";
static char *vhost = "/";
static char *routing_key = "qwe";

static amqp_conn_t *conn = NULL;

static int shutting_down = 0;
static int mode = 0;

#ifndef SIGINFO
UNUSED
#endif
static void
myinfo(UNUSED int sig)
{
    mrkthr_dump_all_ctxes();
}


static void
_shutdown(void)
{
    mrkamqp_fini();
    mrkthr_shutdown();
}


static int
sigshutdown(UNUSED int argc, UNUSED void **argv)
{
    if (!shutting_down) {
        TRACE("Shutting down. Another signal will cause immediate exit.");
        shutting_down = 1;
        if (conn != NULL) {
            amqp_conn_close(conn);
        }
        amqp_conn_destroy(&conn);
        _shutdown();
    } else {
        TRACE("Exiting (sigshutdown)...");
        exit(0);
    }
    return 0;
}


static void
myterm(UNUSED int sig)
{
    (void)mrkthr_spawn("sigshutdown", sigshutdown, 0);
}


void usage(char *path)
{
    printf("Usage: %s "
           "[ -h ] "
           "( -c | -s ) "
           "\n", basename(path));
}


static void
myhandler(UNUSED const amqp_header_t *hin,
          UNUSED const char *din,
          amqp_header_t **hout,
          char **dout,
          UNUSED void *udata)
{
#define MYDLEN 32
    *hout = amqp_header_new();
    (*hout)->body_size = MYDLEN;
    (*dout) = malloc(MYDLEN);
    (void)snprintf(*dout, MYDLEN, "OK");
}


static int
run_conn(void)
{
    int res;
    amqp_channel_t *chan;
    amqp_rpc_t *rpc;

    res = 0;
    assert(conn != NULL);

    if ((chan = amqp_create_channel(conn)) == NULL) {
        res = 1;
        goto err;
    }

    if ((rpc = amqp_rpc_new(NULL, routing_key, NULL)) == NULL) {
        res = 1;
        goto err;
    }

    if (mode != 0) {
        if(amqp_rpc_setup_client(rpc, chan) != 0) {
            res = 1;
            goto err;
        }

        (void)amqp_rpc_run_spawn(rpc);

        while (!shutting_down) {
            char *reply;
            size_t sz;
            int res;

            bytes_t *request;

            request = bytes_printf("test %ld", mrkthr_get_now());
            reply = NULL;
            sz = 0;
            res = amqp_rpc_call(rpc, request, &reply, &sz, 2000);
            BYTES_DECREF(&request);
            D8(reply, sz);
            if (reply != NULL) {
                free(reply);
            }
            if (res != 0) {
                if (res != MRKTHR_WAIT_TIMEOUT) {
                    CTRACE("breaking loop ...");
                    break;
                } else {
                    CTRACE("timeout, skipping ...");
                }
            }
            mrkthr_sleep(1000);
        }

    } else {
        if(amqp_rpc_setup_server(rpc, chan, myhandler, NULL) != 0) {
            res = 1;
            goto err;
        }

        if (amqp_rpc_run(rpc) != 0) {
            res = 1;
            goto err;
        }
    }

    amqp_rpc_teardown(rpc);

    amqp_rpc_destroy(&rpc);

end:
    if (conn != NULL) {
        (void)amqp_conn_close(conn);
    }
    amqp_conn_destroy(&conn);
    return res;

err:
    TR(res);
    goto end;
}


static int
create_conn(void)
{
    int res;

    res = 0;

    conn = amqp_conn_new("10.1.2.10", 5672, user, password, vhost, 0, 0, 0);

    if (amqp_conn_open(conn) != 0) {
        res = 1;
        goto err;
    }

    if (amqp_conn_run(conn) != 0) {
        res = 1;
        goto err;
    }

end:
    return res;

err:
    TR(res);
    amqp_conn_close(conn);
    amqp_conn_destroy(&conn);
    goto end;
}


static int
run0(UNUSED int argc, UNUSED void **argv)
{
    int res;
    assert(argc == 0);

    res = 0;

    mrkamqp_init();

    while (!shutting_down) {
        if (create_conn() != 0) {
            goto err;
        }
        if (run_conn() != 0) {
            goto err;
        }

err:
        assert(conn == NULL);
        CTRACE("Reconnecting ...");
        mrkthr_sleep(1000);
        mrkthr_set_retval(0);
        continue;
    }

    CTRACE("Exiting run0 ...");
    return res;
}


int
main(int argc, char **argv)
{
    int ch;

    if (signal(SIGINT, myterm) == SIG_ERR) {
        return 1;
    }
    if (signal(SIGTERM, myterm) == SIG_ERR) {
        return 1;
    }
#ifdef SIGINFO
    if (signal(SIGINFO, myinfo) == SIG_ERR) {
        return 1;
    }
#endif

    while ((ch = getopt(argc, argv, "chs")) != -1) {
        switch (ch) {
        case 'c':
            mode = 1;
            break;

        case 'h':
            usage(argv[0]);
            exit(0);
            break;

        case 's':
            mode = 0;
            break;

        default:
            FAIL("main");
            break;
        }
    }

    argc -= optind;
    argv += optind;

    mrkthr_init();

    mrkthr_spawn("run0", run0, 0);

    mrkthr_loop();

    _shutdown();
    mrkthr_fini();

    return 0;
}

