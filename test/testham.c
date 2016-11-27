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
#include <mrkcommon/stqueue.h>

#include <mrkthr.h>

#include <mrkamqp_private.h>

#include "diag.h"
#include "mygauge.h"

#define DEFAULT_QSIZE 20
static struct {
    amqp_conn_t *conn;
    amqp_channel_t *chan;
    bool initialized;
} g;

static void fin0(void);

static bool shutting_down = false;
static mrkthr_ctx_t *run_thread = NULL;
static mrkthr_ctx_t *drain_thread = NULL;
static mrkthr_ctx_t *monitor_thread = NULL;
static mrkthr_cond_t mycond;

static char *host = NULL;
static long port = 0;
static char *user = NULL;
static char *password = NULL;
static char *vhost = NULL;
static char *routing_key = NULL;

static uint64_t drain_sleep = 50;
static long qsize = DEFAULT_QSIZE;
static int myqos = 0;
static int nconsumers = 1;

static mygauge_t consumed[128];
static mygauge_t drained;

typedef struct _mymessage {
    STQUEUE_ENTRY(_mymessage, link);
    char *data;
} mymessage_t;


STQUEUE(_mymessage, qmsg);

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
    fin0();

    if (run_thread != NULL) {
        (void)mrkthr_set_interrupt_and_join(run_thread);
        run_thread = NULL;
    }
    if (drain_thread != NULL) {
        (void)mrkthr_set_interrupt_and_join(drain_thread);
        drain_thread = NULL;
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


static void
myterm(UNUSED int sig)
{
    CTRACE("Shutting down ...");
    shutting_down = true;
    MRKTHR_SPAWN_SIG("_shutdown", _shutdown);
}



static void
mymessage_enqueue(char *data)
{
    mymessage_t *msg;

    if ((msg = malloc(sizeof(mymessage_t))) == NULL) {
        FAIL("malloc");
    }

    STQUEUE_ENTRY_INIT(link, msg);
    msg->data = data;
    STQUEUE_ENQUEUE(&qmsg, link, msg);
}


static int
recalc_myqos(void)
{
    int res;
    long diff;

    if (qsize <= 0) {
        return 0;
    }

    diff = qsize - (long)STQUEUE_LENGTH(&qmsg);

    res = 0;
    if (diff > 0) {
        if (myqos <= diff) {
            //CTRACE("myqos: %d->%ld", myqos, diff);
            myqos = diff;
            res = 1;
        }
    } else {
        if (myqos > 1) {
            //CTRACE("myqos: %d->%d", myqos, 1);
            myqos = 1;
            res = 1;
            /* XXX */
        }
    }
    return res;
}


UNUSED static int
mymonitor(UNUSED int argc, UNUSED void **argv)
{
    while (!shutting_down) {
        int i;

        if (mrkthr_sleep(1000) != 0) {
            break;
        }

        TRACEC("consumed:");
        for (i = 0; i < nconsumers; ++i) {
            TRACEC("% 6ld", mygauge_flush(&consumed[i]));
        }
        TRACEC("\tdrained:%ld qlen:%ld qos:%d\n",
               mygauge_flush(&drained),
               STQUEUE_LENGTH(&qmsg),
               myqos);
    }
    return 0;
}


static int
mydrain(UNUSED int argc, UNUSED void **argv)
{
    while (!g.initialized) {
        mrkthr_sleep(1000);
    }

    while (!shutting_down) {
        mymessage_t *msg;

        if ((msg = STQUEUE_HEAD(&qmsg)) != NULL) {
            STQUEUE_DEQUEUE(&qmsg, link);
            STQUEUE_ENTRY_FINI(link, msg);
            assert(msg->data != NULL);
            free(msg->data);
            msg->data = NULL;
            free(msg);
            mygauge_incr(&drained, 1);
        } else {
            mrkthr_cond_signal_one(&mycond);
        }

        mrkthr_sleep(drain_sleep);

        if (recalc_myqos() != 0) {
            if (amqp_channel_qos(g.chan, 0, myqos, 0) != 0) {
                CTRACE("amqp_channel_qos");
                break;
            }
        }
    }

    return 0;
}


static void
fin0(void)
{
    if (g.initialized) {
        CTRACE("closing all");
        g.initialized = false;
        amqp_close_channel(g.chan);
        amqp_conn_close(g.conn, 0);
        amqp_conn_post_close(g.conn);
        amqp_conn_destroy(&g.conn);
        mrkamqp_fini();
        mrkthr_cond_fini(&mycond);
        CTRACE("closed");
    }
}


static int
mycb(UNUSED amqp_frame_t *method,
     UNUSED amqp_frame_t *header,
     char *data,
     void *udata)
{
    int i;
    int res;

    i = (int)(intptr_t)udata;
    res = 0;
    if (data != NULL) {
        if ((qsize > 0) && ((long)STQUEUE_LENGTH(&qmsg) > qsize)) {
            /*
             * qmsg is over limit
             */
            if (mrkthr_cond_wait(&mycond) != 0) {
                TRACE("mrkthr_cond_wait");
            }
            //if (mrkthr_sleep(2000) != 0) {
            //    return 1;
            //}
        }

        mymessage_enqueue(data);
        mygauge_incr(&consumed[i], 1);
    }

    return res;

}


static int
mycbnoq(UNUSED amqp_frame_t *method,
        UNUSED amqp_frame_t *header,
        char *data,
        void *udata)
{
    int i;
    int res;

    i = (int)(intptr_t)udata;
    res = 0;
    if (data != NULL) {
        if ((qsize > 0) && ((long)STQUEUE_LENGTH(&qmsg) > qsize)) {
            /*
             * qmsg is over limit
             */
            if (mrkthr_cond_wait(&mycond) != 0) {
                TRACE("mrkthr_cond_wait");
            }
            //if (mrkthr_sleep(2000) != 0) {
            //    return 1;
            //}
        }

        free(data);
        mygauge_incr(&consumed[i], 1);
    }

    return res;

}


static int
runcons(UNUSED int argc, void **argv)
{
    int i;
    amqp_consumer_t *cons;

    assert(argc == 1);
    i = (int)(intptr_t)argv[0];

    if ((cons = amqp_channel_create_consumer(g.chan,
                                             routing_key,
                                             NULL,
                                             0)) == NULL) {
        goto err;
    }

    CTRACE(">>> consumer %d", i);

    if (qsize > 0) {
        (void)amqp_consumer_handle_content_spawn(cons, mycb, NULL, argv[0]);
    } else {
        (void)amqp_consumer_handle_content_spawn(cons, mycbnoq, NULL, argv[0]);
    }

end:
    CTRACE("<<< consumer %d", i);
    return 0;

err:
    goto end;
}


UNUSED static int
run0(UNUSED int argc, UNUSED void **argv)
{
    int i;

    mygauge_init(&drained, 0);

    mrkthr_cond_init(&mycond);
    mrkamqp_init();

    g.conn = amqp_conn_new(host, port, user, password, vhost, 0, 0, 0, 0);

    if (amqp_conn_open(g.conn) != 0) {
        goto err;
    }

    if (amqp_conn_run(g.conn) != 0) {
        goto err;
    }

    if ((g.chan = amqp_create_channel(g.conn)) == NULL) {
        goto err;
    }

    if (qsize > 0) {
        if (amqp_channel_qos(g.chan, 0, myqos, 0) != 0) {
            goto err;
        }
    }

    g.initialized = true;

    for (i = 0; i < nconsumers; ++i) {
        mrkthr_ctx_t *thread;

        mygauge_init(&consumed[i], 0);
        thread = MRKTHR_SPAWN(NULL, runcons, (void *)(intptr_t)i);
        mrkthr_set_name(thread, "cons%d", i);
    }

end:
    return 0;

err:
    fin0();
    goto end;
}


int
main(int argc, char **argv)
{
    int ch;

    while ((ch = getopt(argc, argv, "C:H:p:P:Q:R:S:V:U:")) != -1) {
        switch (ch) {
        case 'C':
            nconsumers = strtol(optarg, NULL, 10);
            break;

        case 'H':
            host = strdup(optarg);
            break;

        case 'p':
            port = strtol(optarg, NULL, 10);
            break;

        case 'Q':
            qsize = strtol(optarg, NULL, 10);
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
            drain_sleep = strtol(optarg, NULL, 10);
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

    STQUEUE_INIT(&qmsg);

    mrkthr_init();

    run_thread = MRKTHR_SPAWN("run0", run0);
    drain_thread = MRKTHR_SPAWN("drain", mydrain);
    monitor_thread = MRKTHR_SPAWN("monitor", mymonitor);

    mrkthr_loop();
    mrkthr_fini();
    TRACE("Exiting main ...");
    return 0;
}
