#include <assert.h>
#include <err.h>
#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <mrkcommon/bytestream.h>
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkthr.h>

#include <mrkamqp_private.h>

#include "diag.h"
#include "mygauge.h"


static bool shutting_down = false;
static mrkthr_ctx_t *run_thread = NULL;
static mrkthr_ctx_t *monitor_thread = NULL;

static long payload_size = 1024;
static char *host = NULL;
static long port = 0;
static char *user = NULL;
static char *password = NULL;
static char *vhost = NULL;
static char *routing_key = NULL;

static uint64_t publish_sleep = 50;
static mygauge_t published;

#ifndef SIGINFO
UNUSED
#endif
static void
myinfo(UNUSED int sig)
{
    mrkthr_dump_all_ctxes();
}


static int
_shutdown(UNUSED int argc, UNUSED void **argv)
{
    if (run_thread != NULL) {
        (void)mrkthr_set_interrupt_and_join(run_thread);
        run_thread = NULL;
    }
    if (monitor_thread != NULL) {
        (void)mrkthr_set_interrupt_and_join(monitor_thread);
        monitor_thread = NULL;
    }

    /* cleanup */
    if (host != NULL) {
        free(host);
        host = NULL;
    }
    port = 0;
    if (user != NULL) {
        free(user);
        user = NULL;
    }
    if (password != NULL) {
        free(password);
        password = NULL;
    }
    if (vhost != NULL) {
        free(vhost);
        vhost = NULL;
    }
    if (routing_key != NULL) {
        free(routing_key);
        routing_key = NULL;
    }

    mrkthr_shutdown();
    CTRACE("...shutdown OK");
    return 0;
}


static int
sigshutdown(UNUSED int argc, UNUSED void **argv)
{

    CTRACE("Shutting down ...");
    shutting_down = true;
    mrkthr_spawn("_shutdown", _shutdown, 0);
    return 0;
}


static void
myterm(UNUSED int sig)
{
    (void)mrkthr_spawn_sig("sigshutdown", sigshutdown, 0);
}


static int
mymonitor(UNUSED int argc, UNUSED void **argv)
{
    while (!shutting_down) {
        if (mrkthr_sleep(1000) != 0) {
            break;
        }
        TRACEC("pub:%ld\n", mygauge_flush(&published));
    }
    return 0;
}


static void
mycb(UNUSED amqp_channel_t *chan,
     amqp_header_t *header,
     UNUSED void *udata)
{
    header->delivery_mode = 2;
}


static int
run0(UNUSED int argc, UNUSED void **argv)
{
    amqp_conn_t *conn;
    amqp_channel_t *chan;
    bytestream_t bs;

    mrkamqp_init();
    conn = amqp_conn_new(host, port, user, password, vhost, 0, 0, 0, AMQP_CAP_PUBLISHER_CONFIRMS);

    if (amqp_conn_open(conn) != 0) {
        goto err;
    }
    if (amqp_conn_run(conn) != 0) {
        goto err;
    }

    if ((chan = amqp_create_channel(conn)) == NULL) {
        goto err;
    }

    bytestream_init(&bs, payload_size);

    while (!shutting_down) {
        UNUSED int res;
        UNUSED double before, after;
        UNUSED uint64_t before_tocks, after_ticks;

        before = MRKTHR_GET_NOW_PRECISE_FSEC();
        res = amqp_channel_publish(chan,
                                   "",
                                   routing_key,
                                   0,
                                   mycb,
                                   NULL,
                                   (char *)SDATA(&bs, 0),
                                   payload_size);
        after = MRKTHR_GET_NOW_PRECISE_FSEC();
        //CTRACE("res=%d time=%lf", res, after - before);
        mrkthr_sleep(publish_sleep);
        mygauge_incr(&published, 1);
    }

end:
    amqp_conn_close(conn, 0);
    amqp_conn_post_close(conn);
    amqp_conn_destroy(&conn);
    mrkamqp_fini();
    bytestream_fini(&bs);

    return 0;

err:
    goto end;
}


int
main(int argc, char **argv)
{
    int ch;

    while ((ch = getopt(argc, argv, "H:p:P:R:s:S:V:U:")) != -1) {
        switch (ch) {
        case 's':
            payload_size = strtol(optarg, NULL, 10);
            break;

        case 'H':
            host = strdup(optarg);
            break;

        case 'p':
            port = strtol(optarg, NULL, 10);
            break;

        case 'U':
            user = strdup(optarg);
            break;

        case 'P':
            password = strdup(optarg);
            break;

        case 'V':
            vhost = strdup(optarg);
            break;

        case 'R':
            routing_key = strdup(optarg);
            break;

        case 'S':
            publish_sleep = strtol(optarg, NULL, 10);
            break;

        default:
            break;
        }
    }

    argc -= optind;
    argv += optind;

    if (host == NULL) {
        host = strdup("localhost");
    }

    if (port == 0) {
        port = 5672;
    }

    if (user == NULL) {
        user = strdup("guest");
    }

    if (password == NULL) {
        password = strdup("guest");
    }

    if (vhost == NULL) {
        vhost = strdup("/");
    }

    if (routing_key == NULL) {
        errx(1, "Routing key required");
    }

    if (signal(SIGINT, myterm) == SIG_ERR) {
        return 1;
    }
    if (signal(SIGTERM, myterm) == SIG_ERR) {
        return 1;
    }
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        return 1;
    }
#ifdef SIGINFO
    if (signal(SIGINFO, myinfo) == SIG_ERR) {
        return 1;
    }
#endif

    mrkthr_init();

    run_thread = mrkthr_spawn("run0", run0, 0);
    monitor_thread = mrkthr_spawn("monitor", mymonitor, 0);

    mrkthr_loop();
    mrkthr_fini();
    TRACE("Exiting main ...");
    return 0;
}
