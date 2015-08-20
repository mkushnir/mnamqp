#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif
#ifdef HAVE_ENDIAN_H
#   include <endian.h>
#else
#   ifdef HAVE_SYS_ENDIAN_H
#       include <sys/endian.h>
#   else
#       error "Neither endian.h nor sys/endian.h found"
#   endif
#endif

#include <assert.h>

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h> // IPTOS_LOWDELAY

#include <mrkcommon/bytestream.h>
#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/stqueue.h>
#include <mrkcommon/util.h>

#include <mrkthr.h>
#include "mrkamqp_private.h"

#include "diag.h"

static amqp_channel_t *amqp_channel_new(amqp_conn_t *);
static int amqp_channel_destroy(amqp_channel_t **);
static amqp_consumer_t *amqp_consumer_new(amqp_channel_t *);
static int amqp_consumer_item_fini(void *, amqp_consumer_t *);
static int channel_expect_method(amqp_channel_t *,
                                 amqp_meth_id_t,
                                 amqp_frame_t **);
static void channel_send_frame(amqp_channel_t *, amqp_frame_t *);

amqp_conn_t *
amqp_conn_new(const char *host,
              int port,
              const char *user,
              const char *password,
              const char *vhost,
              int heartbeat,
              int frame_max)
{
    amqp_conn_t *conn;

    if ((conn = malloc(sizeof(amqp_conn_t))) == NULL) {
        FAIL("malloc");
    }

    conn->host = strdup(host);
    conn->port = port;
    conn->user = strdup(user);
    conn->password = strdup(password);
    conn->vhost = strdup(vhost);
    conn->heartbeat = heartbeat;
    conn->frame_max = frame_max;

    conn->fd = -1;
    bytestream_init(&conn->ins, 65536);
    conn->ins.read_more = mrkthr_bytestream_read_more;
    bytestream_init(&conn->outs, 65536);
    conn->outs.write = mrkthr_bytestream_write;
    conn->recv_thread = NULL;

    array_init(&conn->channels, sizeof(amqp_channel_t *), 0,
               NULL,
               (array_finalizer_t)amqp_channel_destroy);
    return conn;
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
        UNUSED int optval;

        if ((conn->fd = socket(ai->ai_family,
                               ai->ai_socktype,
                               ai->ai_protocol)) < 0) {
            continue;
        }

        //optval = IPTOS_LOWDELAY;
        //if (setsockopt(conn->fd,
        //               IPPROTO_IP,
        //               IP_TOS,
        //               &optval,
        //               sizeof(optval)) != 0) {
        //    FAIL("setsockopt");
        //}

        //optval = 1;
        //if (setsockopt(conn->fd,
        //               SOL_SOCKET,
        //               SO_SNDLOWAT,
        //               &optval,
        //               sizeof(optval)) != 0) {
        //    FAIL("setsockopt");
        //}


        if (connect(conn->fd, ai->ai_addr, ai->ai_addrlen) != 0) {
            perror("connect");
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


static void
receive_octets(amqp_conn_t *conn, amqp_frame_t *fr)
{
    ssize_t nread, need;

    nread = 0;
    assert(fr->payload.body == NULL);
    if ((fr->payload.body = malloc(fr->sz)) == NULL) {
        FAIL("malloc");
    }
    while (nread < fr->sz) {
        if (SNEEDMORE(&conn->ins)) {
            if (bytestream_consume_data(&conn->ins, conn->fd) != 0) {
                break;
            }
        }
        need = MIN(fr->sz - nread, SEOD(&conn->ins) - SPOS(&conn->ins));
        memcpy(fr->payload.body + nread, SPDATA(&conn->ins), need);
        SADVANCEPOS(&conn->ins, need);
        nread += need;
    }
}


static int
next_frame(amqp_conn_t *conn)
{
    int res;
    off_t spos;
    uint8_t eof;
    amqp_frame_t *fr;
    amqp_channel_t **chan;

    res = 0;
    fr = amqp_frame_new(0, 0);

    if (unpack_octet(&conn->ins, conn->fd, &fr->type) < 0) {
        res = UNPACK + 200;
        goto err;
    }
    //CTRACE("type=%hhd", fr->type);

    if (unpack_short(&conn->ins, conn->fd, &fr->chan) < 0) {
        res = UNPACK + 201;
        goto err;
    }
    //CTRACE("chan=%hd", fr->chan);

    if (unpack_long(&conn->ins, conn->fd, &fr->sz) < 0) {
        res = UNPACK + 202;
        goto err;
    }
    //CTRACE("sz=%d", fr->sz);

    spos = SPOS(&conn->ins);

    while (SAVAIL(&conn->ins) < fr->sz) {
        if (bytestream_consume_data(&conn->ins, conn->fd) != 0) {
            res = UNPACK_ECONSUME;
            goto err;
        }
    }

    SADVANCEPOS(&conn->ins, fr->sz);
    if (unpack_octet(&conn->ins, conn->fd, &eof) < 0) {
        res = UNPACK + 203;
        goto err;
    }

    if (eof != 0xce) {
        CTRACE("eof=%02hhx", eof);
        res = UNPACK + 204;
        goto err;
    }

    /* rewind bs at payload start and ... */
    SPOS(&conn->ins) = spos;

    if ((chan = array_get(&conn->channels, fr->chan)) == NULL) {
        res = UNPACK + 205;
        goto err;
    }

    assert(*chan != NULL);

    switch (fr->type) {
    case AMQP_FMETHOD:
        {
            uint16_t cls, meth;
            amqp_meth_id_t mid;

            if (unpack_short(&conn->ins, conn->fd, &cls) < 0) {
                res = UNPACK + 210;
                goto err;
            }

            if (unpack_short(&conn->ins, conn->fd, &meth) < 0) {
                res = UNPACK + 211;
                goto err;
            }

            mid = AMQP_METHID(cls, meth);

            fr->payload.params = NULL;
            if (amqp_meth_params_decode(conn, mid, &fr->payload.params) != 0) {
                res = UNPACK + 212;
                goto err;
            }

            /*
             * async vs sync methods handling
             */
            if (fr->payload.params->mi->mid == AMQP_BASIC_DELIVER) {
                amqp_basic_deliver_t *m;
                dict_item_t *dit;

                m = (amqp_basic_deliver_t *)fr->payload.params;
                if ((dit = dict_get_item(&(*chan)->consumers,
                                         m->consumer_tag)) == NULL) {
                    CTRACE("cannot find consumer: %s, discarding frame",
                          m->consumer_tag->data);
                    amqp_frame_destroy_method(&fr);

                } else {
                    amqp_consumer_t *cons;

                    cons = dit->value;
                    if (cons->content_method != NULL) {
                        /*
                         * XXX
                         */
                        amqp_frame_destroy_method(&fr);
                    } else {
                        (*chan)->content_consumer = cons;
                        cons->content_method = fr;
                        mrkthr_signal_send(&cons->recvsig);
                    }
                }
            } else {
                STQUEUE_ENQUEUE(&(*chan)->frames, link, fr);
                mrkthr_signal_send(&(*chan)->recvsig);
            }
        }
        break;

    case AMQP_FHEADER:
        {
            amqp_consumer_t *cons;

            if (amqp_header_decode(conn, &fr->payload.header) != 0) {
                res = UNPACK + 220;
                goto err;
            }

            cons = (*chan)->content_consumer;
            if (cons == NULL) {
                CTRACE("got header, not found consumer, discarding frame");
                amqp_frame_destroy_header(&fr);

            } else  {
                if (cons->content_header != NULL) {
                    /*
                     * XXX
                     */
                    amqp_frame_destroy_header(&fr);
                } else {
                    uint16_t class_id;

                    assert(cons->content_method != NULL);
                    class_id = (uint16_t)(cons->
                                          content_method->
                                          payload.params->
                                          mi->
                                          mid >> 16);
                    if (fr->payload.header->class_id != class_id) {
                        /*
                         * XXX
                         */
                        amqp_frame_destroy_header(&fr);

                    } else {
                        amqp_frame_destroy_header(&cons->content_header);
                        cons->content_header = fr;
                        mrkthr_signal_send(&cons->recvsig);
                    }
                }
            }
        }
        break;

    case AMQP_FBODY:
        {
            amqp_consumer_t *cons;

            cons = (*chan)->content_consumer;

            /*
             * XXX implement it
             */
            receive_octets(conn, fr);
            if (cons == NULL) {
                CTRACE("got header, not found consumer, discarding frame");
                amqp_frame_destroy_body(&fr);

            } else {
                if (cons->content_method == NULL ||
                    cons->content_header == NULL) {
                    /*
                     * XXX
                     */
                    amqp_frame_destroy_body(&fr);

                } else {
                    STQUEUE_ENQUEUE(&cons->content_body, link, fr);
                    mrkthr_signal_send(&cons->recvsig);
                }
            }
        }
        break;

    case AMQP_FHEARTBEAT:
        if (fr->chan != 0) {
            res = UNPACK + 230;
            goto err; // 501 frame error
        }
        break;

    default:
        res = UNPACK + 250;
        goto err;
    }

    /*
     * compansate eof octet
     */

    SADVANCEPOS(&conn->ins, 1);

#ifdef TRRET_DEBUG_VERBOSE
    TRACEC("<<< ");
    amqp_frame_dump(fr);
    TRACEC("\n");
#endif


end:
    return res;

err:
    amqp_frame_destroy(&fr);
    TR(res);
    goto end;
}


static void
pack_frame(amqp_conn_t *conn, amqp_frame_t *fr)
{
    off_t seod0, seod1;
    union {
        uint32_t *i;
        char *c;
    } u;

    pack_octet(&conn->outs, fr->type);
    pack_short(&conn->outs, fr->chan);

    seod0 = SEOD(&conn->outs);
    pack_long(&conn->outs, fr->sz);

    seod1 = SEOD(&conn->outs);

    switch (fr->type) {
    case AMQP_FMETHOD:
        assert(fr->payload.params != NULL);
        pack_short(&conn->outs,
                   (uint16_t)(fr->payload.params->mi->mid >> 16));
        pack_short(&conn->outs,
                   (uint16_t)(fr->payload.params->mi->mid & 0xffff));
        fr->payload.params->mi->enc(fr->payload.params, conn);
        break;

    case AMQP_FHEADER:
    case AMQP_FBODY:
    case AMQP_FHEARTBEAT:
    default:
        break;
    }

    u.c = SDATA(&conn->outs, seod0);
    *u.i = htobe32((uint32_t)SEOD(&conn->outs) - seod1);

    pack_octet(&conn->outs, 0xce);

    //D8(SDATA(&conn->outs, 0), SEOD(&conn->outs));
}


static int
recv_thread(UNUSED int argc, void **argv)
{
    amqp_conn_t *conn;

    assert(argc == 1);
    conn = argv[0];
    while (1) {
        if (next_frame(conn) != 0) {
            break;
        }
        /*
         * will no longer read this time
         */
        //CTRACE("SAVAIL=%ld SPOS=%lx SEOD=%lx", SAVAIL(&conn->ins), SPOS(&conn->ins), SEOD(&conn->ins));
        //D32(SDATA(&conn->ins, 0), SEOD(&conn->ins));
        if (SNEEDMORE(&conn->ins)) {
            bytestream_rewind(&conn->ins);
        }

        conn->since_last_frame = mrkthr_get_now();
    }
    return 0;
}


static void
send_octets(amqp_conn_t *conn, uint8_t *octets, size_t sz)
{
    bytestream_rewind(&conn->outs);
    (void)bytestream_cat(&conn->outs, sz, (char *)octets);
    (void)bytestream_produce_data(&conn->outs, conn->fd);
}


int
amqp_conn_run(amqp_conn_t *conn)
{
    int res;

    char greeting[] = {'A', 'M', 'Q', 'P', 0x00, 0x00, 0x09, 0x01};
    amqp_frame_t *fr0, *fr1;
    amqp_connection_start_ok_t *start_ok;
    amqp_connection_tune_t *tune;
    amqp_connection_tune_ok_t *tune_ok;
    amqp_connection_open_t *opn;
    amqp_value_t *caps;
    size_t sz0, sz1;

    res = 0;
    fr0 = NULL;
    fr1 = NULL;

    conn->chan0 = amqp_channel_new(conn);
    assert(conn->chan0->id == 0);
    conn->chan0->closed = 1; /* trick */
    conn->recv_thread = mrkthr_spawn("recvthr", recv_thread, 1, conn);

    // >>> AMQP0091
    send_octets(conn, (uint8_t *)greeting, sizeof(greeting));

    // <<< connection_start
    if (channel_expect_method(conn->chan0, AMQP_CONNECTION_START, &fr0) != 0) {
        res = AMQP_CONN_RUN + 1;
        goto err;
    }
    amqp_frame_destroy_method(&fr0);

    // >>> connection_start_ok
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    start_ok = NEWREF(connection_start_ok)();
    table_add_lstr(&start_ok->client_properties,
                   "product",
                   bytes_new_from_str("mrkamqp"));
    table_add_lstr(&start_ok->client_properties,
                   "version",
                   bytes_new_from_str("0.1"));
    table_add_lstr(&start_ok->client_properties,
                   "information",
                   bytes_new_from_str("http://"));
    caps = amqp_value_new(AMQP_TTABLE);
    init_table(&caps->value.t);
    table_add_bool(&caps->value.t, "publisher_confirms", 1);
    table_add_value(&start_ok->client_properties,
                    "capabilities",
                    caps);
    start_ok->mechanism = bytes_new_from_str("PLAIN");
    sz0 = strlen(conn->user);
    sz1 = strlen(conn->password);
    start_ok->response = bytes_new(2 + sz0 + sz1);
    start_ok->response->data[0] = '\0';
    memcpy(&start_ok->response->data[1], conn->user, sz0);
    start_ok->response->data[1 + sz0] = '\0';
    memcpy(&start_ok->response->data[2 + sz0], conn->password, sz1);
    start_ok->locale = bytes_new_from_str("en_US");
    fr1->payload.params = (amqp_meth_params_t *)start_ok;
    channel_send_frame(conn->chan0, fr1);
    amqp_frame_destroy_method(&fr1);

    // <<< connection_tune
    if (channel_expect_method(conn->chan0, AMQP_CONNECTION_TUNE, &fr0) != 0) {
        res = AMQP_CONN_RUN + 2;
        goto err;
    }
    tune = (amqp_connection_tune_t *)fr0->payload.params;

    // >>> connection_tune_ok
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    tune_ok = NEWREF(connection_tune_ok)();
    tune_ok->channel_max = tune->channel_max;
    tune_ok->frame_max = tune->frame_max;
    //tune_ok->frame_max = conn->frame_max;
    tune_ok->heartbeat = tune->heartbeat;
    //tune_ok->heartbeat = conn->heartbeat;
    amqp_frame_destroy_method(&fr0);

    fr1->payload.params = (amqp_meth_params_t *)tune_ok;
    channel_send_frame(conn->chan0, fr1);
    amqp_frame_destroy_method(&fr1);

    // >>> connection_open
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    opn = NEWREF(connection_open)();
    assert(conn->vhost != NULL);
    opn->virtual_host = bytes_new_from_str(conn->vhost);
    opn->capabilities = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)opn;
    channel_send_frame(conn->chan0, fr1);
    amqp_frame_destroy_method(&fr1);

    // <<< connection_open_ok
    if (channel_expect_method(conn->chan0,
                              AMQP_CONNECTION_OPEN_OK,
                              &fr0) != 0) {
        res = AMQP_CONN_RUN + 3;
        goto err;
    }

end:
    amqp_frame_destroy_method(&fr0);
    amqp_frame_destroy_method(&fr1);
    return res;

err:
    TR(res);
    goto end;
}


static void
amqp_conn_close_fd(amqp_conn_t *conn)
{
    if (conn->fd != -1) {
        close(conn->fd);
        conn->fd = -1;
    }
}


static int
close_channel_cb(amqp_channel_t **chan, UNUSED void *udata)
{
    assert(*chan != NULL);
    (void)amqp_close_channel(*chan);
    return 0;
}

int
amqp_conn_close(amqp_conn_t *conn)
{
    int res;
    amqp_frame_t *fr0, *fr1;
    amqp_connection_close_t *clo;

    res = 0;
    fr0 = NULL;
    fr1 = NULL;

    (void)array_traverse(&conn->channels,
                   (array_traverser_t)close_channel_cb, NULL);

    // >>> connection_close
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    clo = NEWREF(connection_close)();
    clo->reply_text = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)clo;
    channel_send_frame(conn->chan0, fr1);
    amqp_frame_destroy_method(&fr1);

    // <<< connection_close_ok
    if (channel_expect_method(conn->chan0,
                              AMQP_CONNECTION_CLOSE_OK,
                              &fr0) != 0) {
        res = AMQP_CONN_CLOSE + 1;
        goto err;
    }

end:
    amqp_frame_destroy_method(&fr0);
    amqp_frame_destroy_method(&fr1);
    amqp_conn_close_fd(conn);
    return res;

err:
    TR(res);
    goto end;
}


void
amqp_conn_destroy(amqp_conn_t **conn)
{
    if ((*conn) != NULL) {
        array_fini(&(*conn)->channels);

        amqp_conn_close_fd(*conn);
        if ((*conn)->recv_thread != NULL) {
            mrkthr_join((*conn)->recv_thread);
        }
        bytestream_fini(&(*conn)->ins);
        bytestream_fini(&(*conn)->outs);

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


/*
 * channel
 */
static amqp_channel_t *
amqp_channel_new(amqp_conn_t *conn)
{
    amqp_channel_t **chan;

    if ((chan = array_incr(&conn->channels)) == NULL) {
        FAIL("array_incr");
    }

    if ((*chan = malloc(sizeof(amqp_channel_t))) == NULL) {
        FAIL("malloc");
    }
    (*chan)->conn = conn;
    (*chan)->id = conn->channels.elnum - 1;
    (*chan)->confirm_mode = 0;
    (*chan)->closed = 0;
    STQUEUE_INIT(&(*chan)->frames);
    mrkthr_signal_init(&(*chan)->recvsig, NULL);
    dict_init(&(*chan)->consumers, 17,
              (dict_hashfn_t)bytes_hash,
              (dict_item_comparator_t)bytes_cmp,
              (dict_item_finalizer_t)amqp_consumer_item_fini);
    (*chan)->content_consumer = NULL;
    return *chan;
}


static void
channel_send_frame(amqp_channel_t *chan, amqp_frame_t *fr)
{
#ifdef TRRET_DEBUG_VERBOSE
    TRACEC(">>> ");
    amqp_frame_dump(fr);
    TRACEC("\n");
#endif
    bytestream_rewind(&chan->conn->outs);
    pack_frame(chan->conn, fr);
    (void)bytestream_produce_data(&chan->conn->outs, chan->conn->fd);
}


static int
channel_expect_method(amqp_channel_t *chan,
                      amqp_meth_id_t mid,
                      amqp_frame_t **fr)
{
    assert(chan->recvsig.owner == NULL ||
           chan->recvsig.owner == mrkthr_me());

    mrkthr_signal_init(&chan->recvsig, mrkthr_me());

    if (mrkthr_signal_subscribe(&chan->recvsig) != 0) {
        TRRET(CHANNEL_EXPECT_METHOD + 1);
    }

    while ((*fr = STQUEUE_HEAD(&chan->frames)) != NULL) {
        STQUEUE_DEQUEUE(&chan->frames, link);
        STQUEUE_ENTRY_FINI(link, *fr);

        assert((*fr)->chan == chan->id);

        if ((*fr)->type == AMQP_FMETHOD) {
            if ((*fr)->payload.params->mi->mid == mid) {
                /* ok */
            } else {
                amqp_method_info_t *mi;

                mi = amqp_method_info_get(mid);
                assert(mi != NULL);
                CTRACE("expected method: %s, found: %s",
                      mi->name,
                      (*fr)->payload.params->mi->name);
                TRRET(CHANNEL_EXPECT_METHOD + 3);
            }

        } else {
            CTRACE("frame support not implemented");
            amqp_frame_destroy(fr);
            TRRET(CHANNEL_EXPECT_METHOD + 4);
        }
        /*
         * return one frame per call
         */
        break;
    }

    return 0;
}


static int
amqp_channel_destroy(amqp_channel_t **chan)
{
    if (*chan != NULL) {
        if (mrkthr_signal_has_owner(&(*chan)->recvsig)) {
            mrkthr_signal_send(&(*chan)->recvsig);
            mrkthr_signal_fini(&(*chan)->recvsig);
            dict_fini(&(*chan)->consumers);
        }
        free(*chan);
        *chan = NULL;
    }
    return 0;
}


amqp_channel_t *
amqp_create_channel(amqp_conn_t *conn)
{
    amqp_frame_t *fr0, *fr1;
    amqp_channel_t *chan;
    amqp_channel_open_t *opn;

    fr0 = NULL;
    fr1 = NULL;
    chan = amqp_channel_new(conn);

    // >>> channel_open
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    opn = NEWREF(channel_open)();
    opn->out_of_band = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)opn;
    channel_send_frame(conn->chan0, fr1);
    amqp_frame_destroy_method(&fr1);

    // <<< channel_open_ok
    if (channel_expect_method(chan, AMQP_CHANNEL_OPEN_OK, &fr0) != 0) {
        TR(AMQP_CREATE_CHANNEL + 1);
        goto err;
    }
    if (fr0->chan != chan->id) {
        TR(AMQP_CREATE_CHANNEL + 2);
        goto err;
    }

end:
    amqp_frame_destroy_method(&fr0);
    amqp_frame_destroy_method(&fr1);
    return chan;

err:
    amqp_channel_destroy(&chan);
    goto end;
}


#define AMQP_CHANNEL_METHOD_PAIR(mname, okmid, errid, __a1)    \
    int res;                                                   \
    amqp_frame_t *fr0, *fr1;                                   \
    amqp_##mname##_t *m;                                       \
    res = 0;                                                   \
    fr0 = NULL;                                                \
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);              \
    m = NEWREF(mname)();                                       \
    __a1                                                       \
    fr1->payload.params = (amqp_meth_params_t *)m;             \
    channel_send_frame(chan, fr1);                             \
    amqp_frame_destroy_method(&fr1);                           \
    if (channel_expect_method(chan, okmid, &fr0) != 0) {       \
        res = errid + 1;                                       \
        goto err;                                              \
    }                                                          \
end:                                                           \
    amqp_frame_destroy_method(&fr0);                           \
    amqp_frame_destroy_method(&fr1);                           \
    return res;                                                \
err:                                                           \
    TR(res);                                                   \
    goto end;                                                  \



#define AMQP_CHANNEL_METHOD_PAIR_NOWAIT(mname,                 \
                                        fnowait,               \
                                        okmid,                 \
                                        errid,                 \
                                        __a1)                  \
    int res;                                                   \
    amqp_frame_t *fr0, *fr1;                                   \
    amqp_##mname##_t *m;                                       \
    res = 0;                                                   \
    fr0 = NULL;                                                \
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);              \
    m = NEWREF(mname)();                                       \
    fr1->payload.params = (amqp_meth_params_t *)m;             \
    __a1                                                       \
    channel_send_frame(chan, fr1);                             \
    amqp_frame_destroy_method(&fr1);                           \
    if (!(flags & fnowait)) {                                  \
        if (channel_expect_method(chan, okmid, &fr0) != 0) {   \
            res = errid + 1;                                   \
            goto err;                                          \
        }                                                      \
    }                                                          \
end:                                                           \
    amqp_frame_destroy_method(&fr0);                           \
    amqp_frame_destroy_method(&fr1);                           \
    return res;                                                \
err:                                                           \
    TR(res);                                                   \
    goto end;                                                  \



int
amqp_channel_confirm(amqp_channel_t *chan, uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(confirm_select,
                                    CHANNEL_CONFIRM_FNOWAIT,
                                    AMQP_CONFIRM_SELECT_OK,
                                    AMQP_CCONFIRM,
        m->flags = flags;
        chan->confirm_mode = 1;
    )
}


int
amqp_channel_declare_exchange(amqp_channel_t *chan,
                              const char *exchange,
                              const char *type,
                              uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(exchange_declare,
                                    DECLARE_EXCHANGE_FNOWAIT,
                                    AMQP_EXCHANGE_DECLARE_OK,
                                    AMQP_DECLARE_EXCHANGE,
        assert(exchange != NULL);
        assert(type != NULL);
        m->exchange = bytes_new_from_str(exchange);
        m->type = bytes_new_from_str(type);
        m->flags = flags;
    )
}


int
amqp_channel_declare_exchange_ex(amqp_channel_t *chan,
                                 const char *exchange,
                                 const char *type,
                                 uint8_t flags,
                                 amqp_frame_completion_cb_t cb,
                                 void *udata)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(exchange_declare,
                                    DECLARE_EXCHANGE_FNOWAIT,
                                    AMQP_EXCHANGE_DECLARE_OK,
                                    AMQP_DECLARE_EXCHANGE,
        assert(exchange != NULL);
        assert(type != NULL);
        m->exchange = bytes_new_from_str(exchange);
        m->type = bytes_new_from_str(type);
        m->flags = flags;
        assert(cb != NULL);
        cb(chan, fr1, udata);
    )
}



int
amqp_channel_delete_exchange(amqp_channel_t *chan,
                             const char *exchange,
                             uint8_t flags)
{


    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(exchange_delete,
                                    DELETE_EXCHANGE_FNOWAIT,
                                    AMQP_EXCHANGE_DELETE_OK,
                                    AMQP_DELETE_EXCHANGE,
        assert(exchange != NULL);
        m->exchange = bytes_new_from_str(exchange);
        m->flags = flags;
    )
}


int
amqp_channel_declare_queue(amqp_channel_t *chan,
                           const char *queue,
                           uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_declare,
                                    DECLARE_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_DECLARE_OK,
                                    AMQP_DECLARE_QUEUE,
        assert(queue != NULL);
        m->queue = bytes_new_from_str(queue);
        m->flags = flags;
    )
}


int
amqp_channel_declare_queue_ex(amqp_channel_t *chan,
                              const char *queue,
                              uint8_t flags,
                              amqp_frame_completion_cb_t cb,
                              void *udata)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_declare,
                                    DECLARE_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_DECLARE_OK,
                                    AMQP_DECLARE_QUEUE,
        assert(queue != NULL);
        m->queue = bytes_new_from_str(queue);
        m->flags = flags;
        assert(cb != NULL);
        cb(chan, fr1, udata);
    )
}


int
amqp_channel_bind_queue(amqp_channel_t *chan,
                        const char *queue,
                        const char *exchange,
                        const char *routing_key,
                        uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_bind,
                                    BIND_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_BIND_OK,
                                    AMQP_BIND_QUEUE,
        assert(queue != NULL);
        assert(exchange != NULL);
        assert(routing_key != NULL);
        m->queue = bytes_new_from_str(queue);
        m->exchange = bytes_new_from_str(exchange);
        m->routing_key = bytes_new_from_str(routing_key);
        m->flags = flags;
    )
}


int
amqp_channel_bind_queue_ex(amqp_channel_t *chan,
                           const char *queue,
                           const char *exchange,
                           const char *routing_key,
                           uint8_t flags,
                           amqp_frame_completion_cb_t cb,
                           void *udata)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_bind,
                                    BIND_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_BIND_OK,
                                    AMQP_BIND_QUEUE,
        assert(queue != NULL);
        assert(exchange != NULL);
        assert(routing_key != NULL);
        m->queue = bytes_new_from_str(queue);
        m->exchange = bytes_new_from_str(exchange);
        m->routing_key = bytes_new_from_str(routing_key);
        m->flags = flags;
        assert(cb != NULL);
        cb(chan, fr1, udata);
    )
}


int
amqp_channel_unbind_queue(amqp_channel_t *chan,
                          const char *queue,
                          const char *exchange,
                          const char *routing_key)
{
    AMQP_CHANNEL_METHOD_PAIR(queue_unbind,
                             AMQP_QUEUE_UNBIND_OK,
                             AMQP_UNBIND_QUEUE,
        assert(queue != NULL);
        assert(exchange != NULL);
        assert(routing_key != NULL);
        m->queue = bytes_new_from_str(queue);
        m->exchange = bytes_new_from_str(exchange);
        m->routing_key = bytes_new_from_str(routing_key);
    )
}


int
amqp_channel_unbind_queue_ex(amqp_channel_t *chan,
                             const char *queue,
                             const char *exchange,
                             const char *routing_key,
                             amqp_frame_completion_cb_t cb,
                             void *udata)
{
    AMQP_CHANNEL_METHOD_PAIR(queue_unbind,
                             AMQP_QUEUE_UNBIND_OK,
                             AMQP_UNBIND_QUEUE,
        assert(queue != NULL);
        assert(exchange != NULL);
        assert(routing_key != NULL);
        m->queue = bytes_new_from_str(queue);
        m->exchange = bytes_new_from_str(exchange);
        m->routing_key = bytes_new_from_str(routing_key);
        assert(cb != NULL);
        cb(chan, fr1, udata);
    )
}


int
amqp_channel_purge_queue(amqp_channel_t *chan,
                         const char *queue,
                         uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_purge,
                                    PURGE_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_PURGE_OK,
                                    AMQP_PURGE_QUEUE,
        assert(queue != NULL);
        m->queue = bytes_new_from_str(queue);
        m->flags = flags;
    )
}


int
amqp_channel_delete_queue(amqp_channel_t *chan,
                          const char *queue,
                          uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_delete,
                                    DELETE_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_DELETE_OK,
                                    AMQP_DELETE_QUEUE,
        assert(queue != NULL);
        m->queue = bytes_new_from_str(queue);
        m->flags = flags;
    )
}


int
amqp_channel_qos(amqp_channel_t *chan,
                 uint32_t prefetch_size,
                 uint16_t prefetch_count,
                 uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR(basic_qos,
                             AMQP_BASIC_QOS_OK,
                             AMQP_QOS,
        m->prefetch_size = prefetch_size;
        m->prefetch_count = prefetch_count;
        m->flags = flags;
    )
}


/*
 * XXX
 * consumer
 */
int
amqp_channel_consume(amqp_channel_t *chan,
                     const char *queue,
                     const char *consumer_tag,
                     uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(basic_consume,
                                    CONSUME_FNOWAIT,
                                    AMQP_BASIC_CONSUME_OK,
                                    AMQP_CONSUME,
        assert(queue != NULL);
        assert(consumer_tag != NULL);
        m->queue = bytes_new_from_str(queue);
        m->consumer_tag = bytes_new_from_str(consumer_tag);
        m->flags = flags;
    )
}


int
amqp_channel_cancel(amqp_channel_t *chan,
                    const char *consumer_tag,
                    uint8_t flags)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(basic_cancel,
                                    CANCEL_FNOWAIT,
                                    AMQP_BASIC_CANCEL_OK,
                                    AMQP_CANCEL,
        assert(consumer_tag != NULL);
        m->consumer_tag = bytes_new_from_str(consumer_tag);
        m->flags = flags;
    )
}


static int
close_consumer_cb(UNUSED bytes_t *key,
                  amqp_consumer_t*cons,
                  UNUSED void *udata)
{
    assert(cons != NULL);
    (void)amqp_close_consumer(cons);
    return 0;
}

int
amqp_close_channel(amqp_channel_t *chan)
{
    int res;
    amqp_frame_t *fr0, *fr1;
    amqp_channel_close_t *clo;

    res = 0;
    fr0 = NULL;
    fr1 = NULL;

    if (chan->closed) {
        goto end;
    }

    (void)dict_traverse(&chan->consumers,
                        (dict_traverser_t)close_consumer_cb, NULL);

    // >>> channel_close
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    clo = NEWREF(channel_close)();
    clo->reply_text = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)clo;
    channel_send_frame(chan, fr1);
    amqp_frame_destroy_method(&fr1);

    // <<< channel_close_ok
    if (channel_expect_method(chan, AMQP_CHANNEL_CLOSE_OK, &fr0) != 0) {
        res = AMQP_CLOSE_CHANNEL + 1;
        goto err;
    }
    if (fr0->chan != chan->id) {
        res = AMQP_CREATE_CHANNEL + 2;
        goto err;
    }

    chan->closed = 1;

end:
    amqp_frame_destroy_method(&fr0);
    amqp_frame_destroy_method(&fr1);
    return res;

err:
    TR(res);
    goto end;
}

/*
 * consumer
 */

static amqp_consumer_t *
amqp_consumer_new(amqp_channel_t *chan)
{
    amqp_consumer_t *cons;

    if ((cons = malloc(sizeof(amqp_consumer_t))) == NULL) {
        FAIL("malloc");
    }

    cons->chan = chan;
    cons->consumer_tag = NULL;
    cons->content_method = NULL;
    cons->content_header = NULL;
    STQUEUE_INIT(&cons->content_body);
    mrkthr_signal_init(&cons->recvsig, NULL);
    cons->content_thread = NULL;
    cons->content_cb = NULL;
    cons->content_udata = NULL;
    cons->closed = 0;

    return cons;
}


static void
amqp_consumer_destroy(amqp_consumer_t **cons)
{
    if (*cons != NULL) {
        BYTES_DECREF(&(*cons)->consumer_tag);
        amqp_frame_destroy_method(&(*cons)->content_method);
        amqp_frame_destroy_header(&(*cons)->content_header);
        if (mrkthr_signal_has_owner(&(*cons)->recvsig)) {
            mrkthr_signal_send(&(*cons)->recvsig);
            mrkthr_signal_fini(&(*cons)->recvsig);
        }
        STQUEUE_FINI(&(*cons)->content_body);
        free(*cons);
    }
}


static int
amqp_consumer_item_fini(UNUSED void *key, amqp_consumer_t *cons)
{
    amqp_consumer_destroy(&cons);
    return 0;
}



amqp_consumer_t *
amqp_channel_create_consumer(amqp_channel_t *chan,
                             const char *queue,
                             const char *consumer_tag)
{
    dict_item_t *dit;
    amqp_consumer_t *cons;
    amqp_frame_t *fr0, *fr1;
    amqp_basic_consume_t *m;
    amqp_basic_consume_ok_t *ok;

    cons = amqp_consumer_new(chan);

    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    m = NEWREF(basic_consume)();
    assert(queue != NULL);
    m->queue = bytes_new_from_str(queue);
    m->consumer_tag = bytes_new_from_str(
            consumer_tag != NULL ? consumer_tag : "");
    fr1->payload.params = (amqp_meth_params_t *)m;
    channel_send_frame(chan, fr1);
    amqp_frame_destroy_method(&fr1);

    fr0 = NULL;
    if (channel_expect_method(chan, AMQP_BASIC_CONSUME_OK, &fr0) != 0) {
        TR(CHANNEL_CREATE_CONSUMER + 1);
        goto err;
    }

    /* pass consumer_tag from ok to cons */
    ok = (amqp_basic_consume_ok_t *)fr0->payload.params;
    assert(ok->consumer_tag != NULL);
    cons->consumer_tag = ok->consumer_tag;
    ok->consumer_tag = NULL;

    if ((dit = dict_get_item(&chan->consumers, cons->consumer_tag)) != NULL) {
        TR(CHANNEL_CREATE_CONSUMER + 2);
        goto err;
    }
    dict_set_item(&chan->consumers, cons->consumer_tag, cons);

end:
    amqp_frame_destroy_method(&fr0);
    amqp_frame_destroy_method(&fr1);
    return cons;

err:
    amqp_consumer_destroy(&cons);
    goto end;
}


static int
content_thread_worker(UNUSED int argc, void **argv)
{
    assert(argc == 1);
    amqp_consumer_t *cons;

    cons = argv[0];

    //CTRACE("consumer %s listening ...", cons->consumer_tag->data);
    while (1) {
        amqp_frame_t *method, *header, *body;
        char *data;

        //CTRACE("new state ...");
        method = amqp_consumer_get_method(cons);
        //CTRACE("got method");
        header = amqp_consumer_get_header(cons);
        if ((data = malloc(header->payload.header->body_size)) == NULL) {
            FAIL("malloc");
        }
        //CTRACE("got header");

        while (header->payload.header->body_size >
               header->payload.header->received_size) {
            size_t sz0;

            //TRACE("bs=%ld rs=%ld",
            //      header->payload.header->body_size,
            //      header->payload.header->received_size);
            sz0 = header->payload.header->received_size;
            body = amqp_consumer_get_body(cons);
            //CTRACE("got body");
            memcpy(data + sz0, body->payload.body, body->sz);
            amqp_frame_destroy(&body);
        }

        assert(cons->content_cb != NULL);
        cons->content_cb(method, header, data, cons->content_udata);
        free(data);

        amqp_consumer_reset_content_state(cons);

    }

    return 0;
}

void
amqp_consumer_handle_content(amqp_consumer_t *cons,
                             amqp_consumer_content_cb_t cb,
                             void *udata)
{
    cons->content_cb = cb;
    cons->content_udata = udata;
    cons->content_thread = mrkthr_spawn((char *)cons->consumer_tag->data,
                                        content_thread_worker,
                                        1,
                                        cons);
    mrkthr_join(cons->content_thread);

}

amqp_frame_t *
amqp_consumer_get_method(amqp_consumer_t *cons)
{
    assert(cons->recvsig.owner == NULL ||
           cons->recvsig.owner == mrkthr_me());

    mrkthr_signal_init(&cons->recvsig, mrkthr_me());
    while (cons->content_method == NULL) {
        mrkthr_signal_subscribe(&cons->recvsig);
    }
    return cons->content_method;
}


amqp_frame_t *
amqp_consumer_get_header(amqp_consumer_t *cons)
{
    assert(cons->recvsig.owner == NULL ||
           cons->recvsig.owner == mrkthr_me());

    mrkthr_signal_init(&cons->recvsig, mrkthr_me());
    while (cons->content_header == NULL) {
        mrkthr_signal_subscribe(&cons->recvsig);
    }
    return cons->content_header;
}


amqp_frame_t *
amqp_consumer_get_body(amqp_consumer_t *cons)
{
    amqp_frame_t *fr;

    assert(cons->recvsig.owner == NULL ||
           cons->recvsig.owner == mrkthr_me());

    mrkthr_signal_init(&cons->recvsig, mrkthr_me());
    while ((fr = STQUEUE_HEAD(&cons->content_body)) == NULL) {
        mrkthr_signal_subscribe(&cons->recvsig);
    }
    STQUEUE_DEQUEUE(&cons->content_body, link);
    STQUEUE_ENTRY_FINI(link, fr);
    cons->content_header->payload.header->received_size +=
        fr->sz;
    return fr;
}


void
amqp_consumer_reset_content_state(amqp_consumer_t *cons)
{
    amqp_frame_t *fr1, *fr;
    amqp_basic_ack_t *m;
    amqp_basic_deliver_t *d;

    fr1 = amqp_frame_new(cons->chan->id, AMQP_FMETHOD);
    m = NEWREF(basic_ack)();
    assert(cons->content_method->payload.params->mi->mid == AMQP_BASIC_DELIVER);
    d = (amqp_basic_deliver_t *)cons->content_method->payload.params;
    m->delivery_tag = d->delivery_tag;
    fr1->payload.params = (amqp_meth_params_t *)m;
    channel_send_frame(cons->chan, fr1);
    amqp_frame_destroy(&fr1);

    amqp_frame_destroy_method(&cons->content_method);
    amqp_frame_destroy_header(&cons->content_header);
    while ((fr = STQUEUE_HEAD(&cons->content_body)) != NULL) {
        STQUEUE_DEQUEUE(&cons->content_body, link);
        STQUEUE_ENTRY_FINI(link, fr);
        amqp_frame_destroy_body(&fr);
    }
}


int
amqp_close_consumer(amqp_consumer_t *cons)
{
    if (!cons->closed) {
        assert(cons->consumer_tag != NULL);
        if (amqp_channel_cancel(cons->chan,
                                (const char *)cons->consumer_tag->data,
                                0) != 0) {
            TR(AMQP_CLOSE_CONSUMER + 1);
        }
        BYTES_DECREF(&cons->consumer_tag);
        cons->closed = 1;
    }
    return 0;
}

