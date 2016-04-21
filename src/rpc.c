#include <assert.h>

#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp_rpc);
#endif

#include <mrkcommon/bytestream.h>
//#define TRRET_DEBUG
//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkamqp_private.h>

#include "diag.h"


typedef struct _rpc_call_completion {
    mrkthr_signal_t sig;
    struct _amqp_rpc *rpc;
    amqp_consumer_content_cb_t response_cb;
    void *udata;
} rpc_call_completion_t;


static int
rpc_call_item_fini(UNUSED bytes_t *key, void *value) {

    rpc_call_completion_t *cc;

    cc = value;
    mrkthr_signal_error(&cc->sig, MRKAMQP_STOP_THREADS);
    return 0;
}


amqp_rpc_t *
amqp_rpc_new(char *exchange,
             char *routing_key,
             char *reply_to)
{
    amqp_rpc_t *rpc;

    assert(routing_key != NULL);

    if ((rpc = malloc(sizeof(amqp_rpc_t))) == NULL) {
        FAIL("malloc");
    }

    if (exchange != NULL) {
        if ((rpc->exchange = strdup(exchange)) == NULL) {
            FAIL("strdup");
        }
    } else {
        if ((rpc->exchange = strdup("")) == NULL) {
            FAIL("strdup");
        }
    }
    if((rpc->routing_key = strdup(routing_key)) == NULL) {
        FAIL("strdup");
    }
    if (reply_to != NULL) {
        rpc->reply_to = bytes_new_from_str(reply_to);
        BYTES_INCREF(rpc->reply_to);
    } else {
        rpc->reply_to = NULL;
    }
    rpc->chan = NULL;
    rpc->cons = NULL;
    hash_init(&rpc->calls,
              101,
              (hash_hashfn_t)bytes_hash,
              (hash_item_comparator_t)bytes_cmp,
              (hash_item_finalizer_t)rpc_call_item_fini);
    rpc->next_id = 0ll;
    rpc->cccb = NULL;
    rpc->clcb = NULL;

    return rpc;
}


void
amqp_rpc_destroy(amqp_rpc_t **rpc)
{
    if ((*rpc) != NULL) {
        (void)amqp_rpc_teardown(*rpc);
        hash_fini(&(*rpc)->calls);
        free((*rpc)->exchange);
        free((*rpc)->routing_key);
        BYTES_DECREF(&(*rpc)->reply_to);
        (*rpc)->chan = NULL;
        free(*rpc);
        *rpc = NULL;
    }
}


static int
amqp_rpc_cancel_cb(UNUSED amqp_frame_t *method,
                   UNUSED amqp_frame_t *header,
                   UNUSED char *data,
                   UNUSED void *udata)
{
    CTRACE("rpc was cancelled");
    return 0;
}

/*
 * server
 */
static int
amqp_rpc_server_cb(UNUSED amqp_frame_t *method,
                   amqp_frame_t *header,
                   char *data,
                   void *udata)
{
    int res;
    amqp_rpc_t *rpc;
    amqp_header_t *callback_header;
    char *callback_data;

    rpc = udata;
    callback_header = NULL;
    callback_data = NULL;

    assert(rpc != NULL);
    rpc->server_handler(header->payload.header,
                        data,
                        &callback_header,
                        &callback_data,
                        rpc->server_udata);

    res = 0;

    if (header->payload.header->reply_to != NULL) {
        if (callback_header != NULL) {
            if (header->payload.header->correlation_id != NULL) {
                AMQP_HEADER_SET_REF(correlation_id)(callback_header,
                        header->payload.header->correlation_id);
            }
            callback_header->class_id = AMQP_BASIC;
            res = amqp_channel_publish_ex(
                    rpc->chan,
                    rpc->exchange,
                    (char *)BDATA(header->payload.header->reply_to),
                    0,
                    callback_header, callback_data);
            /* take header over, no free() on the handler side */
            callback_header = NULL;
        } else {
            CTRACE("server handler returned NULL callback header, "
                   "discarding reply");
        }
    } else {
        amqp_header_destroy(&callback_header);
        CTRACE("no reply_to in the incoming call, discarding server reply");
    }

    if (callback_data != NULL) {
        free(callback_data);
    }
    if (data != NULL) {
        free(data);
    }
    return res;
}


int
amqp_rpc_setup_server(amqp_rpc_t *rpc,
                      amqp_channel_t *chan,
                      amqp_rpc_server_handler_t server_handler,
                      void *server_udata)
{
    int res;

    res = 0;
    rpc->chan = chan;

    if (amqp_channel_declare_queue(chan, rpc->routing_key, 0) != 0) {
        res = AMQP_RPC_SETUP_SERVER + 1;
        goto err;
    }

    if (*rpc->exchange != '\0') {
        if (amqp_channel_bind_queue(chan,
                                    rpc->routing_key,
                                    rpc->exchange,
                                    rpc->routing_key,
                                    0) != 0) {
            res = AMQP_RPC_SETUP_SERVER + 2;
            goto err;
        }
    }
    if ((rpc->cons = amqp_channel_create_consumer(chan,
                                                  rpc->routing_key,
                                                  NULL,
                                                  CONSUME_FNOACK)) == NULL) {
        res = AMQP_RPC_SETUP_SERVER + 3;
        goto err;
    }
    rpc->cccb = amqp_rpc_server_cb;
    rpc->clcb = amqp_rpc_cancel_cb;
    rpc->server_handler = server_handler;
    rpc->server_udata = server_udata;

end:
    return res;
err:
    TR(res);
    goto end;
}


/*
 * client
 */
static void
amqp_rpc_setup_client_cb0(UNUSED amqp_channel_t *chan,
                          amqp_frame_t *fr0,
                          void *udata)
{
    amqp_rpc_t *rpc;
    amqp_queue_declare_ok_t *m;

    rpc = udata;
    /* transfer queue reference */
    m = (amqp_queue_declare_ok_t *)fr0->payload.params;
    rpc->reply_to = m->queue;
    m->queue = NULL;

}


static int
amqp_rpc_client_cb(UNUSED amqp_frame_t *method,
                   amqp_frame_t *header,
                   char *data,
                   void *udata)
{
    int res;
    amqp_rpc_t *rpc;

    rpc = udata;

    res = 0;
    if (header->payload.header->correlation_id != NULL) {
        hash_item_t *dit;

        if ((dit = hash_get_item(&rpc->calls,
                        header->payload.header->correlation_id)) == NULL) {
            CTRACE("no pending call for correlation_id %s, ignoring",
                  (char *)BDATA(header->payload.header->correlation_id));
            if (data != NULL) {
                free(data);
            }
        } else {
            rpc_call_completion_t *cc;

            cc = dit->value;
            if (cc->response_cb != NULL) {
                res = cc->response_cb(method, header, data, cc->udata);
            }
            mrkthr_signal_send(&cc->sig);
        }
    } else {
        CTRACE("no correlation_id in header, ignoring:");
        amqp_frame_dump(header);
        if (data != NULL) {
            free(data);
        }
    }
    return res;
}


int
amqp_rpc_setup_client(amqp_rpc_t *rpc, amqp_channel_t *chan)
{
    int res;

    res = 0;
    rpc->chan = chan;
    if (rpc->reply_to == NULL) {
        if (amqp_channel_declare_queue_ex(chan,
                                          "",
                                          DECLARE_QUEUE_FEXCLUSIVE |
                                            DECLARE_EXCHANGE_FAUTODELETE,
                                          NULL,
                                          amqp_rpc_setup_client_cb0,
                                          rpc) != 0) {
            res = AMQP_RPC_SETUP_CLIENT + 1;
            goto err;
        }
        assert(rpc->reply_to != NULL);
        if (*rpc->exchange != '\0') {
            if (amqp_channel_bind_queue(chan,
                                        (char *)BDATA(rpc->reply_to),
                                        rpc->exchange,
                                        (char *)BDATA(rpc->reply_to),
                                        0) != 0) {
                res = AMQP_RPC_SETUP_CLIENT + 2;
                goto err;
            }
        }
    }
    if ((rpc->cons = amqp_channel_create_consumer(chan,
                                                  (char *)BDATA(rpc->reply_to),
                                                  NULL,
                                                  CONSUME_FNOACK)) == NULL) {
        res = AMQP_RPC_SETUP_CLIENT + 3;
        goto err;
    }
    rpc->cccb = amqp_rpc_client_cb;
    rpc->clcb = amqp_rpc_cancel_cb;

end:
    return res;
err:
    TR(res);
    goto end;
}


static void
rpc_call_header_completion_cb(UNUSED amqp_channel_t *chan,
                              amqp_header_t *header,
                              void *udata)
{
    struct {
        bytes_t *reply_to;
        bytes_t *cid;
        amqp_rpc_request_header_cb_t request_header_cb;
        void *header_udata;
    } *params;

    params = udata;
    assert(params->reply_to != NULL);
    assert(params->cid != NULL);

    AMQP_HEADER_SET_REF(reply_to)(header, params->reply_to);
    AMQP_HEADER_SET_REF(correlation_id)(header, params->cid);
    if (params->request_header_cb != NULL) {
        params->request_header_cb(header, params->header_udata);
    }
}


int
amqp_rpc_call(amqp_rpc_t *rpc,
              const char *request,
              size_t sz,
              amqp_rpc_request_header_cb_t request_header_cb,
              amqp_consumer_content_cb_t response_cb,
              void *header_udata)
{
    int res;
    struct {
        bytes_t *reply_to;
        bytes_t *cid;
        amqp_rpc_request_header_cb_t request_header_cb;
        void *header_udata;
    } params;
    rpc_call_completion_t cc;

    res = 0;
    params.reply_to = rpc->reply_to;
    params.cid = bytes_printf("%016lx", ++rpc->next_id);
    BYTES_INCREF(params.cid); // nref 1
    params.request_header_cb = request_header_cb;
    params.header_udata = header_udata;

    mrkthr_signal_init(&cc.sig, mrkthr_me());
    cc.rpc = rpc;
    cc.response_cb = response_cb;
    cc.udata = header_udata;

    assert(hash_get_item(&rpc->calls, params.cid) == NULL);
    hash_set_item(&rpc->calls, params.cid, &cc);

    if (amqp_channel_publish(rpc->chan,
                             rpc->exchange,
                             rpc->routing_key,
                             0,
                             rpc_call_header_completion_cb,
                             &params, // nref +- 1
                             (char *)request,
                             sz) != 0) {
        res = AMQP_RPC_CALL + 1;
        goto err;
    }

    if (mrkthr_signal_subscribe(&cc.sig) != 0) {
        res = AMQP_RPC_CALL + 2;
        goto err;
    }

end:
    hash_remove_item(&rpc->calls, params.cid);
    BYTES_DECREF(&params.cid); // nref 0
    mrkthr_signal_fini(&cc.sig);
    return res;
err:
    TR(res);
    goto end;
}

/*
 *
 */
int
amqp_rpc_teardown(amqp_rpc_t *rpc)
{
    int res;

    res = 0;
    if (rpc->cons != NULL) {
        if ((res = amqp_close_consumer(rpc->cons)) != 0) {
            TR(res);
        }
        rpc->cons = NULL;
    }
    if (rpc->chan != NULL) {
        if ((res = amqp_channel_delete_queue(rpc->chan,
                                             (char *)BDATA(rpc->reply_to),
                                             0)) != 0) {
            TR(res);
        }
    }
    return res;
}


static int
amqp_rpc_run_spawn_worker(UNUSED int argc, void **argv)
{
    amqp_rpc_t *rpc;

    assert(argc == 1);
    rpc = argv[0];
    return amqp_consumer_handle_content(rpc->cons,
                                        rpc->cccb,
                                        rpc->clcb,
                                        rpc);
}


mrkthr_ctx_t *
amqp_rpc_run_spawn(amqp_rpc_t *rpc)
{
    assert(rpc->cons != NULL);
    assert(rpc->cccb != NULL);
    assert(rpc->clcb != NULL);
    return mrkthr_spawn("amqrpc", amqp_rpc_run_spawn_worker, 1, rpc);
}

int
amqp_rpc_run(amqp_rpc_t *rpc)
{
    assert(rpc->cons != NULL);
    assert(rpc->cccb != NULL);
    assert(rpc->clcb != NULL);
    return amqp_consumer_handle_content(rpc->cons,
                                        rpc->cccb,
                                        rpc->clcb,
                                        rpc);
}


