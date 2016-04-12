#include <assert.h>

#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp_rpc);
#endif

#include <mrkcommon/bytestream.h>
//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkamqp_private.h>

#include "diag.h"

static int
rpc_call_item_fini(UNUSED bytes_t *key, void *value) {

    rpc_call_completion_t *cc;

    cc = value;
    mrkthr_signal_error(&cc->sig, 0x80);
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
    rpc->cb = NULL;

    return rpc;
}


void
amqp_rpc_destroy(amqp_rpc_t **rpc)
{
    if ((*rpc) != NULL) {
        hash_fini(&(*rpc)->calls);
        free((*rpc)->exchange);
        free((*rpc)->routing_key);
        BYTES_DECREF(&(*rpc)->reply_to);
        (*rpc)->chan = NULL;
        (void)amqp_rpc_teardown(*rpc);
        free(*rpc);
        *rpc = NULL;
    }
}


/*
 * server
 */
static void
amqp_rpc_server_cb(UNUSED amqp_frame_t *method,
                   amqp_frame_t *header,
                   char *data,
                   void *udata)
{
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
    if (header->payload.header->reply_to != NULL) {
        if (callback_header != NULL) {
            if (header->payload.header->correlation_id != NULL) {
                AMQP_HEADER_SET_REF(correlation_id)(callback_header,
                        header->payload.header->correlation_id);
                BYTES_INCREF(header->payload.header->correlation_id);
            }
            callback_header->class_id = AMQP_BASIC;
            (void)amqp_channel_publish_ex(
                    rpc->chan,
                    rpc->exchange,
                    (char *)header->payload.header->reply_to->data,
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
    rpc->cb = amqp_rpc_server_cb;
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
    BYTES_INCREF(rpc->reply_to);
    m->queue = NULL;

}


static void
amqp_rpc_client_cb(UNUSED amqp_frame_t *method,
                   amqp_frame_t *header,
                   char *data,
                   void *udata)
{
    amqp_rpc_t *rpc;

    rpc = udata;

    if (header->payload.header->correlation_id != NULL) {
        hash_item_t *dit;

        if ((dit = hash_get_item(&rpc->calls,
                        header->payload.header->correlation_id)) == NULL) {
            CTRACE("no pending call for correlation_id %s, ignoring",
                  (char *)header->payload.header->correlation_id->data);
            if (data != NULL) {
                free(data);
            }
        } else {
            rpc_call_completion_t *cc;

            cc = dit->value;
            cc->data = data; /* passed over to */
            cc->sz = header->payload.header->body_size;
            mrkthr_signal_send(&cc->sig);
        }
    } else {
        CTRACE("no correlation_id in header, ignoring:");
        amqp_frame_dump(header);
        if (data != NULL) {
            free(data);
        }
    }
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
                                          DECLARE_QUEUE_FEXCLUSIVE,
                                          NULL,
                                          amqp_rpc_setup_client_cb0,
                                          rpc) != 0) {
            res = AMQP_RPC_SETUP_CLIENT + 1;
            goto err;
        }
        assert(rpc->reply_to != NULL);
        if (*rpc->exchange != '\0') {
            if (amqp_channel_bind_queue(chan,
                                        (char *)rpc->reply_to->data,
                                        rpc->exchange,
                                        (char *)rpc->reply_to->data,
                                        0) != 0) {
                res = AMQP_RPC_SETUP_CLIENT + 2;
                goto err;
            }
        }
    }
    if ((rpc->cons = amqp_channel_create_consumer(chan,
                                                  (char *)rpc->reply_to->data,
                                                  NULL,
                                                  CONSUME_FNOACK)) == NULL) {
        res = AMQP_RPC_SETUP_CLIENT + 3;
        goto err;
    }
    rpc->cb = amqp_rpc_client_cb;

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
        void (*header_ucb)(amqp_header_t *, void *);
        void *header_udata;
    } *params;

    params = udata;
    assert(params->reply_to != NULL);
    assert(params->cid != NULL);

    AMQP_HEADER_SET_REF(reply_to)(header, params->reply_to);
    BYTES_INCREF(params->reply_to); // nref +1
    AMQP_HEADER_SET_REF(correlation_id)(header, params->cid);
    BYTES_INCREF(params->cid); // nref +1
    if (params->header_ucb != NULL) {
        params->header_ucb(header, params->header_udata);
    }
}


int
amqp_rpc_call(amqp_rpc_t *rpc,
              bytes_t *request,
              void (*header_ucb)(amqp_header_t *, void *),
              void *header_udata,
              char **reply,
              size_t *sz,
              uint64_t timeout)
{
    int res;
    struct {
        bytes_t *reply_to;
        bytes_t *cid;
        void (*header_ucb)(amqp_header_t *, void *);
        void *header_udata;
    } params;
    rpc_call_completion_t cc;

    res = 0;
    params.reply_to = rpc->reply_to;
    params.cid = bytes_printf("%016lx", ++rpc->next_id);
    params.header_ucb = header_ucb;
    params.header_udata = header_udata;
    BYTES_INCREF(params.cid); // nref 1
    cc.rpc = rpc;
    cc.data = NULL;
    cc.sz = 0;
    mrkthr_signal_init(&cc.sig, mrkthr_me());
    assert(hash_get_item(&rpc->calls, params.cid) == NULL);
    hash_set_item(&rpc->calls, params.cid, &cc);

    if (amqp_channel_publish(rpc->chan,
                             rpc->exchange,
                             rpc->routing_key,
                             0,
                             rpc_call_header_completion_cb,
                             &params, // nref +- 1
                             (char *)request->data,
                             request->sz) != 0) {
        res = AMQP_RPC_CALL + 1;
        goto err;
    }

    if ((res = mrkthr_signal_subscribe_with_timeout(&cc.sig, timeout)) != 0) {
        if (res != MRKTHR_WAIT_TIMEOUT) {
            res = AMQP_RPC_CALL + 2;
        }
        goto err;
    }

    *reply = cc.data;
    *sz = cc.sz;

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
        (void)amqp_close_consumer(rpc->cons);
        rpc->cons = NULL;
    }
    return res;
}


static int
amqp_rpc_run_spawn_worker(UNUSED int argc, void **argv)
{
    amqp_rpc_t *rpc;

    assert(argc == 1);
    rpc = argv[0];
    return amqp_consumer_handle_content(rpc->cons, rpc->cb, rpc);
}


mrkthr_ctx_t *
amqp_rpc_run_spawn(amqp_rpc_t *rpc)
{
    assert(rpc->cons != NULL);
    assert(rpc->cb != NULL);
    return mrkthr_spawn("rpcspawn", amqp_rpc_run_spawn_worker, 1, rpc);
}

int
amqp_rpc_run(amqp_rpc_t *rpc)
{
    assert(rpc->cons != NULL);
    assert(rpc->cb != NULL);
    return amqp_consumer_handle_content(rpc->cons, rpc->cb, rpc);
}


