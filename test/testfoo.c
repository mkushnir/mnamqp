#include <assert.h>
#include <signal.h>

#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>

#include <mrkamqp_private.h>
#include <mrkthr.h>

#include "diag.h"

#include "unittest.h"

#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp_testfoo);
#endif

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

static amqp_conn_t *conn = NULL;

static void
test0(void)
{
    struct {
        long rnd;
        int in;
        int expected;
    } data[] = {
        {0, 0, 0},
    };
    UNITTEST_PROLOG_RAND;

    FOREACHDATA {
        //TRACE("in=%d expected=%d", CDATA.in, CDATA.expected);
        assert(CDATA.in == CDATA.expected);
    }
}


static int
_shutdown(UNUSED int argc, UNUSED void **argv)
{
    amqp_conn_close(conn);
    amqp_conn_destroy(&conn);
    mrkamqp_fini();
    //mrkthr_fini();
    exit(0);
    return 0;
}


static void
myterm(UNUSED int sig)
{
    (void)mrkthr_spawn("shutdown_thread", _shutdown, 0);
}

UNUSED static int
mypub(UNUSED int argc, void **argv)
{
    UNUSED amqp_channel_t *chan;

    assert(argc == 1);
    chan = argv[0];

    while (1) {
        char buf[1024];

        mrkthr_sleep(15000);
        snprintf(buf, sizeof(buf), "data %ld", mrkthr_get_now());
        TRACEC("%s", buf);
        //if (amqp_channel_publish(chan,
        //                         "",
        //                         "qwe",
        //                         0,
        //                         NULL,
        //                         NULL,
        //                         buf,
        //                         strlen(buf)) != 0) {
        //    break;
        //}
    }
    CTRACE("Exiting mypub...");
    return 0;
}


static void
my_content_cb(UNUSED amqp_frame_t *method,
              amqp_frame_t *header,
              char *data,
              UNUSED void *udata)
{
    TRACE("---");
    if (data != NULL) {
        D8(data, header->payload.header->body_size);
        free(data);
    }
    TRACE("---");
}


static void
declare_queue_cb1(UNUSED amqp_channel_t *chan,
                  amqp_frame_t *fr,
                  UNUSED void *udata)
{
    amqp_queue_declare_t *m;
    //amqp_value_t *args;

    m = (amqp_queue_declare_t *)fr->payload.params;
    //args = amqp_value_new(AMQP_TTABLE);
    //init_table(&args->value.t);
    table_add_i32(&m->arguments, "x-expires", 3600000);
    table_add_lstr(&m->arguments, "x-ha-policy", bytes_new_from_str("all"));
}


static int
run(UNUSED int argc, UNUSED void **argv)
{
    int res;
    amqp_channel_t *chan;
    amqp_consumer_t *cons;
    UNUSED mrkthr_ctx_t *cons_thread;

    res = 0;
    conn = amqp_conn_new("localhost",
                         5672,
                         "guest",
                         "guest",
                         "/",
                         0,
                         0,
                         0);

    if (amqp_conn_open(conn) != 0) {
        res = 1;
        goto err;
    }

    if (amqp_conn_run(conn) != 0) {
        res = 1;
        goto err;
    }

    if ((chan = amqp_create_channel(conn)) == NULL) {
        res = 1;
        goto err;
    }

    if (amqp_channel_confirm(chan, 0) != 0) {
        res = 1;
        goto err;
    }

    //if (amqp_channel_declare_exchange(chan, "qwe", "direct", DECLARE_EXCHANGE_FPASSIVE) != 0) {
    //    res = 1;
    //    goto err;
    //}

    //if (amqp_channel_declare_exchange(chan, "qwe", "direct", 0) != 0) {
    //    res = 1;
    //    goto err;
    //}

    //if (amqp_channel_delete_exchange(chan, "qwe", 0) != 0) {
    //    res = 1;
    //    goto err;
    //}

    //if (amqp_channel_declare_queue(chan, "qwe", DECLARE_QUEUE_FEXCLUSIVE) != 0) {
    //    res = 1;
    //    goto err;
    //}

    if (amqp_channel_declare_queue_ex(chan,
                                      "qwe",
                                      DECLARE_QUEUE_FEXCLUSIVE,
                                      declare_queue_cb1,
                                      NULL,
                                      NULL) != 0) {
        res = 1;
        goto err;
    }

    if ((cons = amqp_channel_create_consumer(chan,
                                             "qwe",
                                             NULL,
                                             CONSUME_FNOACK & 0)) == NULL) {
        res = 1;
        goto err;
    }

    mrkthr_spawn("pub", mypub, 1, chan);

    amqp_consumer_handle_content(cons, my_content_cb, NULL);

    if (amqp_close_consumer(cons)) {
    }

    if (amqp_close_channel(chan) != 0) {
        res = 1;
        goto err;
    }

end:
    amqp_conn_close(conn);
    amqp_conn_destroy(&conn);
    return res;

err:
    TR(res);
    goto end;
}


static void
test1(void)
{

    mrkthr_init();
    mrkamqp_init();

    mrkthr_spawn("run", run, 0);

    mrkthr_loop();

    mrkamqp_fini();
    mrkthr_fini();
}


int
main(void)
{
    if (signal(SIGINT, myterm) == SIG_ERR) {
        return 1;
    }
    if (signal(SIGTERM, myterm) == SIG_ERR) {
        return 1;
    }
    test0();
    test1();
    return 0;
}
