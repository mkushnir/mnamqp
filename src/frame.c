#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp_frame);
#endif

#include <mrkcommon/bytestream.h>
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include "mrkamqp_private.h"

#include "diag.h"

amqp_frame_t *
amqp_frame_new(uint16_t chan, uint8_t type)
{
    amqp_frame_t *res;

    if ((res = malloc(sizeof(amqp_frame_t))) == NULL) {
        FAIL("malloc");
    }
    res->payload.params = NULL;
    STQUEUE_ENTRY_INIT(link, res);
    res->sz = 0;
    res->chan = chan;
    res->type = type;
    return res;
}


void
amqp_frame_dump(amqp_frame_t *fr)
{
    TRACEC("[%hd/%s ", fr->chan, AMQP_FRAME_TYPE_STR(fr->type));
    if (fr->payload.params != NULL) {
        if (fr->type == AMQP_FMETHOD) {
            amqp_meth_params_dump(fr->payload.params);
        } else if (fr->type == AMQP_FHEADER) {
            amqp_header_dump(fr->payload.header);
        } else if (fr->type == AMQP_FBODY) {
            TRACEC("sz=%d", fr->sz);
            //TRACEC("\n");
            //D8(fr->payload.body, fr->sz);
        }
    }
    TRACEC("]");
}


void
amqp_frame_destroy_method(amqp_frame_t **fr)
{
    if (*fr != NULL) {
        amqp_meth_params_destroy(&(*fr)->payload.params);
        free(*fr);
        *fr = NULL;
    }
}


void
amqp_frame_destroy_header(amqp_frame_t **fr)
{
    if (*fr != NULL) {
        amqp_header_destroy(&(*fr)->payload.header);
        free(*fr);
        *fr = NULL;
    }
}


void
amqp_frame_destroy_body(amqp_conn_t *conn, amqp_frame_t **fr)
{
    if (*fr != NULL) {
        if ((*fr)->payload.body != NULL) {
            conn->buffer_free((*fr)->payload.body);
        }
        free(*fr);
        *fr = NULL;
    }
}


void
amqp_frame_destroy(amqp_conn_t *conn, amqp_frame_t **fr)
{
    if (*fr != NULL) {
        switch ((*fr)->type) {
        case AMQP_FMETHOD:
            amqp_meth_params_destroy(&(*fr)->payload.params);
            break;

        case AMQP_FHEADER:
            amqp_header_destroy(&(*fr)->payload.header);
            break;

        case AMQP_FBODY:
            if ((*fr)->payload.body != NULL) {
                conn->buffer_free((*fr)->payload.body);
            }
            break;

        case AMQP_FBODYEX:
            break;
        }

        free(*fr);
        *fr = NULL;
    }
}
