#include <assert.h>
#include <err.h>
#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>

#include <mncommon/bytestream.h>
#include <mncommon/dumpm.h>
#include <mncommon/util.h>

#include <mnthr.h>

#include <mnamqp_private.h>

#include "diag.h"
#include "mygauge.h"


static bool shutting_down = false;
static mnthr_ctx_t *run_thread = NULL;
static mnthr_ctx_t *monitor_thread = NULL;

static size_t lo_payload_size = 1024;
static size_t hi_payload_size = 1024;
static char *host = NULL;
static long port = 0;
static char *user = NULL;
static char *password = NULL;
static char *vhost = NULL;
static char *exchange = NULL;
static char *routing_key = NULL;

static uint64_t publish_sleep = 1;
static mygauge_t published;
static mygauge_t published_bytes;
static mygauge_t oframes;

#ifndef SIGINFO
UNUSED
#endif
static void
myinfo(UNUSED int sig)
{
    mnthr_dump_all_ctxes();
}


static int
_shutdown(UNUSED int argc, UNUSED void **argv)
{
    if (run_thread != NULL) {
        (void)mnthr_set_interrupt_and_join(run_thread);
        run_thread = NULL;
    }
    if (monitor_thread != NULL) {
        (void)mnthr_set_interrupt_and_join(monitor_thread);
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
    if (exchange != NULL) {
        free(exchange);
        exchange = NULL;
    }
    if (routing_key != NULL) {
        free(routing_key);
        routing_key = NULL;
    }

    mnthr_shutdown();
    CTRACE("...shutdown OK");
    return 0;
}


static int
sigshutdown(UNUSED int argc, UNUSED void **argv)
{

    CTRACE("Shutting down ...");
    shutting_down = true;
    MNTHR_SPAWN("_shutdown", _shutdown);
    return 0;
}


static void
myterm(UNUSED int sig)
{
    (void)MNTHR_SPAWN_SIG("sigshutdown", sigshutdown);
}


static int
mymonitor(UNUSED int argc, UNUSED void **argv)
{
    while (!shutting_down) {
        if (mnthr_sleep(1000) != 0) {
            break;
        }
        TRACEC("pub:%ld\t%ld\t%ld\n", mygauge_flush(&published), mygauge_flush(&published_bytes), mygauge_flush(&oframes));
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


#define MNCOMMON_ALIGN_CEILING(x, r) (((x) % (r)) ? ((x) + (r) - (x) % (r)) : x)

static int
run0(UNUSED int argc, UNUSED void **argv)
{
    amqp_conn_t *conn;
    amqp_channel_t *chan;
    mnbytestream_t bs;

    mnamqp_init();
    conn = amqp_conn_new(host,
                         port,
                         user,
                         password,
                         vhost,
                         0,
                         MAX(MNCOMMON_ALIGN_CEILING(hi_payload_size, 1024), 0x20000),
                         0,
                         AMQP_CAP_PUBLISHER_CONFIRMS);

    if (amqp_conn_open(conn) != 0) {
        goto err;
    }
    if (amqp_conn_run(conn) != 0) {
        goto err;
    }

    if ((chan = amqp_create_channel(conn)) == NULL) {
        goto err;
    }

    bytestream_init(&bs, hi_payload_size);

    while (!shutting_down) {
        UNUSED int res;
        UNUSED double before, after;
        UNUSED uint64_t before_tocks, after_ticks;
        ssize_t payload_size;

        if (hi_payload_size != lo_payload_size) {
            payload_size = (ssize_t)(random() % (hi_payload_size - lo_payload_size)) + lo_payload_size;
        } else {
            payload_size = (ssize_t)lo_payload_size;
        }
        before = MNTHR_GET_NOW_FSEC_PRECISE();
        res = amqp_channel_publish(chan,
                                   exchange,
                                   routing_key,
                                   0,
                                   mycb,
                                   NULL,
                                   (char *)SDATA(&bs, 0),
                                   payload_size);
        after = MNTHR_GET_NOW_FSEC_PRECISE();
        //CTRACE("res=%d time=%lf", res, after - before);
        mnthr_sleep(publish_sleep);
        mygauge_incr(&published, 1);
        mygauge_update(&oframes, amqp_conn_oframes_length(conn));
    }

end:
    amqp_conn_close(conn, 0);
    amqp_conn_post_close(conn);
    amqp_conn_destroy(&conn);
    mnamqp_fini();
    bytestream_fini(&bs);

    return 0;

err:
    goto end;
}


static void
usage(char *prog)
{
    printf(
"Usage: %s OPTIONS\n"
"\n"
"Options:\n"
"  -E NAME      exchange to bind to, default empty\n"
"  -H HOST      host to connect to, default localhsot\n"
"  -p PORT      port to bind to, default 5672\n"
"  -U NAME      user name, default guest\n"
"  -P STR       password, default guest\n"
"  -V STR       vhost, default /\n"
"  -R STR       routing key, required\n"
"  -r NUM       high payload size, default 1024\n"
"  -s NUM       low payload size, default 1024\n"
"  -S NUM       sleep time, in msec, when publishing a single message,\n"
"               default 1ms\n"
"  -h           print this message and exit\n",
        basename(prog));
}


int
main(int argc, char **argv)
{
    int ch;

    while ((ch = getopt(argc, argv, "E:hH:p:P:R:r:s:S:V:U:")) != -1) {
        switch (ch) {
        case 's':
            lo_payload_size = strtol(optarg, NULL, 10);
            break;

        case 'r':
            hi_payload_size = strtol(optarg, NULL, 10);
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

        case 'E':
            exchange = strdup(optarg);
            break;

        case 'R':
            routing_key = strdup(optarg);
            break;

        case 'S':
            publish_sleep = strtol(optarg, NULL, 10);
            break;

        case 'h':
            usage(argv[0]);
            exit(0);
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

    if (exchange == NULL) {
        exchange = strdup("");
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

    if (hi_payload_size < lo_payload_size) {
        hi_payload_size = lo_payload_size;
    }

    srandom(time(NULL));
    mnthr_init();

    run_thread = MNTHR_SPAWN("run0", run0);
    monitor_thread = MNTHR_SPAWN("monitor", mymonitor);

    mnthr_loop();
    mnthr_fini();
    TRACE("Exiting main ...");
    return 0;
}
