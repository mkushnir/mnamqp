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
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h> // IPTOS_LOWDELAY

#include <mrkcommon/bytestream.h>
//#define TRRET_DEBUG
//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/stqueue.h>
#include <mrkcommon/util.h>

#include <mrkthr.h>
#include <mrkamqp_private.h>

#include "diag.h"

#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp);
#ifdef BYTES_DECREF
#undef BYTES_DECREF
#define BYTES_DECREF bytes_decref
#endif
#endif

static amqp_channel_t *amqp_channel_new(amqp_conn_t *);
static int amqp_channel_destroy(amqp_channel_t **);
static int channel_expect_method(amqp_channel_t *,
                                 amqp_meth_id_t,
                                 amqp_frame_t **);
static void channel_send_frame(amqp_channel_t *, amqp_frame_t *);
static amqp_consumer_t *amqp_consumer_new(amqp_channel_t *, uint8_t);
static int amqp_consumer_item_fini(bytes_t *, amqp_consumer_t *);
static amqp_pending_content_t *amqp_pending_content_new(void);

amqp_conn_t *
amqp_conn_new(const char *host,
              int port,
              const char *user,
              const char *password,
              const char *vhost,
              short channel_max,
              int frame_max,
              short heartbeat,
              int capabilities)
{
    amqp_conn_t *conn;

    if ((conn = malloc(sizeof(amqp_conn_t))) == NULL) {
        FAIL("malloc");
    }

    if ((conn->host = strdup(host)) == NULL) {
        FAIL("strdup");
    }
    conn->port = port;
    if ((conn->user = strdup(user)) == NULL) {
        FAIL("strdup");
    }
    if ((conn->password = strdup(password)) == NULL) {
        FAIL("strdup");
    }
    if ((conn->vhost = strdup(vhost)) == NULL) {
        FAIL("strdup");
    }
    conn->channel_max = channel_max;
    conn->frame_max = frame_max;
    conn->payload_max = frame_max = 8;
    conn->heartbeat = heartbeat;
    conn->capabilities = capabilities;

    conn->fd = -1;
    bytestream_init(&conn->ins, 65536);
    conn->ins.read_more = mrkthr_bytestream_read_more;
    bytestream_init(&conn->outs, 65536);
    conn->outs.write = mrkthr_bytestream_write;
    conn->recv_thread = NULL;
    conn->send_thread = NULL;
    STQUEUE_INIT(&conn->oframes);
    mrkthr_signal_init(&conn->oframe_sig, NULL);

    //CTRACE("channels init %p", &conn->channels);
    array_init(&conn->channels, sizeof(amqp_channel_t *), 0,
               NULL,
               (array_finalizer_t)amqp_channel_destroy);
    conn->chan0 = NULL;
    conn->error_code = 0;
    conn->error_msg = NULL;
    conn->closed = 1;
    return conn;
}


int
amqp_conn_open(amqp_conn_t *conn)
{
    int res;
    struct addrinfo hints, *ainfos, *ai;
    char portstr[32];

    if (!conn->closed) {
        TRRET(AMQP_CONN_OPEN + 1);
    }

    if (conn->fd >= 0) {
        TRRET(AMQP_CONN_OPEN + 2);
    }

    snprintf(portstr, sizeof(portstr), "%d", conn->port);
    memset(&hints, 0, sizeof(hints));
    //hints.ai_family = PF_INET;
    hints.ai_socktype = SOCK_STREAM;
    //hints.ai_protocol = IPPROTO_TCP;

    ainfos = NULL;
    if (getaddrinfo(conn->host, portstr, &hints, &ainfos) != 0) {
        if (ainfos != NULL) {
            freeaddrinfo(ainfos);
        }
        TRRET(AMQP_CONN_OPEN + 3);
    }

    res = 0;
    for (ai = ainfos; ai != NULL; ai = ai->ai_next) {
        UNUSED int optval;
        UNUSED socklen_t optlen;

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

        //optval = 1;
        //if (setsockopt(conn->fd,
        //               SOL_SOCKET,
        //               SO_REUSEADDR,
        //               &optval,
        //               sizeof(optval)) != 0) {
        //    FAIL("setsockopt");
        //}

        //optval = 1;
        //if (setsockopt(conn->fd,
        //               SOL_SOCKET,
        //               SO_REUSEPORT,
        //               &optval,
        //               sizeof(optval)) != 0) {
        //    FAIL("setsockopt");
        //}

        if (mrkthr_connect(conn->fd, ai->ai_addr, ai->ai_addrlen) != 0) {
            res = AMQP_CONN_OPEN + 4;
            goto err;
        }

        break;
    }

    if (conn->fd < 0) {
        res = AMQP_CONN_OPEN + 5;
        goto err;
    }

    conn->closed = 0;

end:
    freeaddrinfo(ainfos);

    return res;

err:
    TR(res);
    goto end;
}


static
amqp_pending_content_t *
amqp_pending_content_new(void)
{
    amqp_pending_content_t *pc;

    if ((pc = malloc(sizeof(amqp_pending_content_t))) == NULL) {
        FAIL("malloc");
    }
    STQUEUE_ENTRY_INIT(link, pc);
    pc->method = NULL;
    pc->header = NULL;
    STQUEUE_INIT(&pc->body);
    return pc;
}


static void
amqp_pending_content_destroy(amqp_pending_content_t **pc)
{
    if (*pc != NULL) {
        amqp_frame_t *fr;

        amqp_frame_destroy_method(&(*pc)->method);
        amqp_frame_destroy_header(&(*pc)->header);
        while ((fr = STQUEUE_HEAD(&(*pc)->body)) != NULL) {
            STQUEUE_DEQUEUE(&(*pc)->body, link);
            STQUEUE_ENTRY_FINI(link, fr);
            amqp_frame_destroy_body(&fr);
        }
        free(*pc);
        *pc = NULL;
    }
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

    SADVANCEPOS(&conn->ins, fr->sz);
    while (SNEEDMORE(&conn->ins)) {
        if (bytestream_consume_data(&conn->ins, conn->fd) != 0) {
            res = UNPACK_ECONSUME;
            goto err;
        }
    }

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

    //CTRACE("channels get %p", &conn->channels);
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

#ifdef TRRET_DEBUG_VERBOSE
            TRACEC("<<< ");
            amqp_frame_dump(fr);
            TRACEC("\n");
#endif
            /*
             * async vs sync methods handling
             */
            if (fr->payload.params->mi->mid == AMQP_BASIC_DELIVER) {
                amqp_basic_deliver_t *m;
                hash_item_t *dit;

                m = (amqp_basic_deliver_t *)fr->payload.params;
                if ((dit = hash_get_item(&(*chan)->consumers,
                                         m->consumer_tag)) == NULL) {
                    CTRACE("got basic.deliver to %s, "
                           "cannot find, discarding frame",
                           BDATA(m->consumer_tag));
                    amqp_frame_destroy_method(&fr);

                } else {
                    amqp_pending_content_t *pc;

                    (*chan)->content_consumer = dit->value;
                    pc = amqp_pending_content_new();
                    pc->method = fr;
                    STQUEUE_ENQUEUE(
                            &(*chan)->content_consumer->pending_content,
                            link,
                            pc);
                    mrkthr_signal_send(
                            &(*chan)->content_consumer->content_sig);
                }

            } else if (fr->payload.params->mi->mid == AMQP_BASIC_CANCEL) {
                amqp_basic_cancel_t *m;
                hash_item_t *dit;

                m = (amqp_basic_cancel_t *)fr->payload.params;
                if ((dit = hash_get_item(&(*chan)->consumers,
                                         m->consumer_tag)) == NULL) {
                    CTRACE("got basic.cancel to %s, "
                           "cannot find, discarding frame",
                           BDATA(m->consumer_tag));
                    amqp_frame_destroy_method(&fr);

                } else {
                    amqp_pending_content_t *pc;

                    (*chan)->content_consumer = dit->value;
                    pc = amqp_pending_content_new();
                    pc->method = fr;
                    STQUEUE_ENQUEUE(
                            &(*chan)->content_consumer->pending_content,
                            link,
                            pc);
                    mrkthr_signal_send(
                            &(*chan)->content_consumer->content_sig);
                }

            } else if (fr->payload.params->mi->mid == AMQP_BASIC_ACK) {
                amqp_basic_ack_t *m;
                amqp_pending_pub_t *pp;

                m = (amqp_basic_ack_t *)fr->payload.params;
                if (m->flags & ACK_MULTIPLE) {
                    while ((pp =
                            STQUEUE_HEAD(&(*chan)->pending_pub)) != NULL) {
                        STQUEUE_DEQUEUE(&(*chan)->pending_pub, link);
                        STQUEUE_ENTRY_FINI(link, pp);

                        if (m->delivery_tag > pp->publish_tag) {
                            mrkthr_signal_send(&pp->sig);
                        } else if (m->delivery_tag == pp->publish_tag) {
                            mrkthr_signal_send(&pp->sig);
                            break;
                        } else {
                            CTRACE("got basic.ack deliver_tag=%ld "
                                   "expected >=%ld",
                                   m->delivery_tag, pp->publish_tag);
                            mrkthr_signal_error(&pp->sig, MRKAMQP_PROTOCOL_ERROR);
                        }
                    }

                    if (pp == NULL) {
                        CTRACE("got basic.ack deliver_tag=%ld none expected",
                               m->delivery_tag);
                    }

                } else {
                    if ((pp = STQUEUE_HEAD(&(*chan)->pending_pub)) != NULL) {
                        STQUEUE_DEQUEUE(&(*chan)->pending_pub, link);
                        STQUEUE_ENTRY_FINI(link, pp);

                        if (pp->publish_tag == m->delivery_tag) {
                            mrkthr_signal_send(&pp->sig);
                        } else {
                            CTRACE("got basic.ack deliver_tag=%ld expected %ld",
                                   m->delivery_tag, pp->publish_tag);
                            mrkthr_signal_error(&pp->sig, MRKAMQP_PROTOCOL_ERROR);
                        }
                    } else {
                        CTRACE("got basic.ack deliver_tag=%ld none expected",
                               m->delivery_tag);
                    }
                }

                amqp_frame_destroy_method(&fr);

            } else if (fr->payload.params->mi->mid == AMQP_CONNECTION_CLOSE) {
                if ((*chan)->id == 0) {
                    amqp_connection_close_t *cc;
                    amqp_frame_t *fr1;
                    amqp_connection_close_ok_t *m;

                    cc = (amqp_connection_close_t *)fr->payload.params;
                    conn->error_code = cc->reply_code;
                    conn->error_msg = bytes_new_from_bytes(cc->reply_text);

                    fr1 = amqp_frame_new((*chan)->id, AMQP_FMETHOD);
                    m = NEWREF(connection_close_ok)();
                    fr1->payload.params = (amqp_meth_params_t *)m;
                    channel_send_frame(*chan, fr1);

                } else {
                    CTRACE("connection.close on non-zero channel, ignoring.");
                }
                amqp_frame_destroy_method(&fr);

            } else {
                STQUEUE_ENQUEUE(&(*chan)->iframes, link, fr);
                mrkthr_signal_send(&(*chan)->expect_sig);
                //CTRACE("signal sent to expect sig");
            }
        }
        break;

    case AMQP_FHEADER:
        {
            amqp_consumer_t *cons;

            if (amqp_header_dec(conn, &fr->payload.header) != 0) {
                res = UNPACK + 220;
                goto err;
            }

#ifdef TRRET_DEBUG_VERBOSE
            TRACEC("<<< ");
            amqp_frame_dump(fr);
            TRACEC("\n");
#endif
            cons = (*chan)->content_consumer;
            if (cons == NULL) {
                CTRACE("got header, not found consumer, discarding frame");
                amqp_frame_destroy_header(&fr);

            } else  {
                amqp_pending_content_t *pc;

                pc = STQUEUE_TAIL(&cons->pending_content);
                if (pc == NULL) {
                    CTRACE("got header, not found pending content, "
                           "discarding frame");
                    amqp_frame_destroy_header(&fr);
                } else {
                    if (pc->header != NULL) {
                        /*
                         * XXX
                         */
                        CTRACE("duplicate header is not expected "
                               "during delivery, discarding frame");
                        amqp_frame_destroy_header(&fr);
                    } else {
                        uint16_t class_id;

                        assert(pc->method != NULL);
                        class_id = (uint16_t)(pc->
                                              method->
                                              payload.params->
                                              mi->
                                              mid >> 16);
                        if (fr->payload.header->class_id != class_id) {
                            /*
                             * XXX
                             */
                            CTRACE("got class_id %hd, expected %hd, "
                                   "discarding frame",
                                   fr->payload.header->class_id, class_id);
                            amqp_frame_destroy_header(&fr);

                        } else {
                            pc->header = fr;
                            mrkthr_signal_send(&cons->content_sig);
                        }
                    }
                }
            }
        }
        break;

    case AMQP_FBODY:
        {
            amqp_consumer_t *cons;

            /*
             * XXX implement it
             */
            receive_octets(conn, fr);

#ifdef TRRET_DEBUG_VERBOSE
            TRACEC("<<< ");
            amqp_frame_dump(fr);
            TRACEC("\n");
#endif
            cons = (*chan)->content_consumer;

            if (cons == NULL) {
                CTRACE("got body, not found consumer, discarding frame");
                amqp_frame_destroy_body(&fr);

            } else {
                amqp_pending_content_t *pc;

                pc = STQUEUE_TAIL(&cons->pending_content);
                if (pc == NULL) {
                    CTRACE("got body, not found pending content, "
                           "discarding frame");
                    amqp_frame_destroy_body(&fr);
                } else {
                    if (pc->method == NULL || pc->header == NULL) {
                        /*
                         * XXX
                         */
                        CTRACE("found body when no previous method/header, "
                               "discarding frame");
                        amqp_frame_destroy_body(&fr);

                    } else {
                        STQUEUE_ENQUEUE(&pc->body, link, fr);
                        mrkthr_signal_send(&cons->content_sig);
                    }
                }
            }
        }
        break;

    case AMQP_FHEARTBEAT:
        {
            amqp_frame_t *fr1;

#ifdef TRRET_DEBUG_VERBOSE
            TRACEC("<<< ");
            amqp_frame_dump(fr);
            TRACEC("\n");
#endif
            if (fr->chan != 0) {
                res = UNPACK + 230;
                goto err; // 501 frame error
            }

            fr1 = amqp_frame_new((*chan)->id, AMQP_FHEARTBEAT);
            channel_send_frame(*chan, fr1);
            fr1 = NULL;
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

end:
    return res;

err:
    amqp_frame_destroy(&fr);
    TR(res);
    goto end;
}


static int
recv_thread_worker(UNUSED int argc, void **argv)
{
    amqp_conn_t *conn;

    assert(argc == 1);
    conn = argv[0];

    while (!conn->closed) {
        if (next_frame(conn) != 0) {
            break;
        }

        /* will no longer read this time */
        if (SNEEDMORE(&conn->ins)) {
            bytestream_rewind(&conn->ins);
        }
    }

    //CTRACE("Exiting recv_tread ...");
    return 0;
}


static void
pack_frame(amqp_conn_t *conn, amqp_frame_t *fr)
{
    union {
        uint32_t *i;
        char *c;
    } u;

    pack_octet(&conn->outs, fr->type);
    pack_short(&conn->outs, fr->chan);

    switch (fr->type) {
    case AMQP_FMETHOD:
        {
            off_t seod0, seod1;

            seod0 = SEOD(&conn->outs);
            pack_long(&conn->outs, fr->sz);
            seod1 = SEOD(&conn->outs);

            assert(fr->payload.params != NULL);
            pack_short(&conn->outs,
                       (uint16_t)(fr->payload.params->mi->mid >> 16));
            pack_short(&conn->outs,
                       (uint16_t)(fr->payload.params->mi->mid & 0xffff));
            fr->payload.params->mi->enc(fr->payload.params, conn);

            u.c = SDATA(&conn->outs, seod0);
            *u.i = htobe32((uint32_t)SEOD(&conn->outs) - seod1);
        }
        break;

    case AMQP_FHEADER:
        {
            off_t seod0, seod1;

            seod0 = SEOD(&conn->outs);
            pack_long(&conn->outs, fr->sz);
            seod1 = SEOD(&conn->outs);

            assert(fr->payload.header != NULL);
            (void)amqp_header_enc(fr->payload.header, conn);

            u.c = SDATA(&conn->outs, seod0);
            *u.i = htobe32((uint32_t)SEOD(&conn->outs) - seod1);
        }
        break;

    case AMQP_FBODY:
        assert(fr->sz > 0);
        pack_long(&conn->outs, fr->sz);
        assert(fr->payload.body != NULL);
        (void)bytestream_cat(&conn->outs, fr->sz, fr->payload.body);
        break;

    case AMQP_FHEARTBEAT:
        assert(fr->sz == 0);
        pack_long(&conn->outs, fr->sz);
        break;

    default:
        break;
    }

    pack_octet(&conn->outs, 0xce);

    //D16(SDATA(&conn->outs, 0), SEOD(&conn->outs));
}


static int
send_thread_worker(UNUSED int argc, void **argv)
{
    amqp_conn_t *conn;

    assert(argc == 1);
    conn = argv[0];
    mrkthr_signal_init(&conn->oframe_sig, mrkthr_me());
    while (!conn->closed) {
        amqp_frame_t *fr;

        if ((fr = STQUEUE_HEAD(&conn->oframes)) == NULL) {
            if (mrkthr_signal_subscribe(&conn->oframe_sig) != 0) {
                //CTRACE("send_thread interrupted");
                break;
            }
        } else {
            STQUEUE_DEQUEUE(&conn->oframes, link);
            STQUEUE_ENTRY_FINI(link, fr);

#ifdef TRRET_DEBUG_VERBOSE
            TRACEC(">>> ");
            amqp_frame_dump(fr);
            TRACEC("\n");
#endif
            bytestream_rewind(&conn->outs);
            pack_frame(conn, fr);
            amqp_frame_destroy(&fr);
            if (bytestream_produce_data(&conn->outs, conn->fd) != 0) {
                //CTRACE("send_thread interrupted");
                break;
            }
        }
    }
    mrkthr_signal_fini(&conn->oframe_sig);
    //CTRACE("exiting send thread ...");
    return 0;
}


static int
send_raw_octets(amqp_conn_t *conn, uint8_t *octets, size_t sz)
{
    bytestream_rewind(&conn->outs);
    (void)bytestream_cat(&conn->outs, sz, (char *)octets);
    return bytestream_produce_data(&conn->outs, conn->fd);
}


static bytes_t _capabilities = BYTES_INITIALIZER("capabilities");

int
amqp_conn_run(amqp_conn_t *conn)
{
    int res;
    char greeting[] = {'A', 'M', 'Q', 'P', 0x00, 0x00, 0x09, 0x01};
    amqp_frame_t *fr0, *fr1;
    amqp_connection_start_t *_start; //weakref
    hash_t *hisprops; //weakref
    UNUSED amqp_value_t *hiscaps; //weakref
    amqp_connection_start_ok_t *start_ok;
    amqp_connection_tune_t *tune;
    amqp_connection_tune_ok_t *tune_ok;
    amqp_connection_open_t *opn;
    amqp_value_t *mycaps;
    size_t sz0, sz1;

    res = 0;
    fr0 = NULL;

    if (conn->closed) {
        res = AMQP_CONN_RUN + 1;
        goto err;
    }

    conn->chan0 = amqp_channel_new(conn);
    assert(conn->chan0->id == 0);

    if (mrkthr_sema_acquire(&conn->chan0->sync_sema) != 0) {
        res = AMQP_CONN_RUN + 2;
        goto err;
    }

    conn->recv_thread = mrkthr_spawn("amqrcv", recv_thread_worker, 1, conn);
    mrkthr_set_prio(conn->recv_thread, 1);
    conn->send_thread = mrkthr_spawn("amqsnd", send_thread_worker, 1, conn);
    mrkthr_set_prio(conn->send_thread, 1);

    // >>> AMQP0091
    if (send_raw_octets(conn, (uint8_t *)greeting, sizeof(greeting)) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_RUN + 3;
        goto err;
    }

    // <<< connection_start
    if (channel_expect_method(conn->chan0, AMQP_CONNECTION_START, &fr0) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_RUN + 4;
        goto err;
    }
    _start = (amqp_connection_start_t *)fr0->payload.params;
    hisprops = &_start->server_properties;
    hiscaps = table_get_value(hisprops, &_capabilities);

    // >>> connection_start_ok
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    start_ok = NEWREF(connection_start_ok)();
    table_add_lstr(&start_ok->client_properties,
                   "product",
                   bytes_new_from_str(PACKAGE_NAME));
    table_add_lstr(&start_ok->client_properties,
                   "version",
                   bytes_new_from_str(PACKAGE_VERSION));
    table_add_lstr(&start_ok->client_properties,
                   "information",
                   bytes_new_from_str(PACKAGE_URL));

    mycaps = amqp_value_new(AMQP_TTABLE);
    init_table(&mycaps->value.t);
    if (conn->capabilities & AMQP_CAP_PUBLISHER_CONFIRMS) {
        table_add_boolean(&mycaps->value.t, "publisher_confirms", 1);
    }
    if (conn->capabilities & AMQP_CAP_CONSUMER_CANCEL_NOTIFY) {
        table_add_boolean(&mycaps->value.t, "consumer_cancel_notify", 1);
    }
    table_add_value(&start_ok->client_properties,
                    "capabilities",
                    mycaps);
    start_ok->mechanism = bytes_new_from_str("PLAIN");
    sz0 = strlen(conn->user);
    sz1 = strlen(conn->password);
    start_ok->response = bytes_new(3 + sz0 + sz1);
    BDATA(start_ok->response)[0] = '\0';
    memcpy(&BDATA(start_ok->response)[1], conn->user, sz0);
    BDATA(start_ok->response)[1 + sz0] = '\0';
    memcpy(&BDATA(start_ok->response)[2 + sz0], conn->password, sz1);
    start_ok->locale = bytes_new_from_str("en_US");
    fr1->payload.params = (amqp_meth_params_t *)start_ok;
    channel_send_frame(conn->chan0, fr1);
    fr1 = NULL;

    amqp_frame_destroy_method(&fr0);

    // <<< connection_tune
    if (channel_expect_method(conn->chan0, AMQP_CONNECTION_TUNE, &fr0) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_RUN + 5;
        goto err;
    }
    tune = (amqp_connection_tune_t *)fr0->payload.params;

    // >>> connection_tune_ok
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    tune_ok = NEWREF(connection_tune_ok)();
    tune_ok->channel_max = tune->channel_max;
    conn->channel_max = tune->channel_max;
    //tune_ok->channel_max = conn->channel_max;
    tune_ok->frame_max = tune->frame_max;
    conn->frame_max = tune->frame_max;
    conn->payload_max = tune->frame_max - 8;
    //tune_ok->frame_max = conn->frame_max;
    tune_ok->heartbeat = tune->heartbeat;
    conn->heartbeat = tune->heartbeat;
    //tune_ok->heartbeat = conn->heartbeat;
    amqp_frame_destroy_method(&fr0);

    fr1->payload.params = (amqp_meth_params_t *)tune_ok;
    channel_send_frame(conn->chan0, fr1);
    fr1 = NULL;

    if (conn->frame_max <= 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_RUN + 6;
        goto err;
    }

    // >>> connection_open
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    opn = NEWREF(connection_open)();
    assert(conn->vhost != NULL);
    opn->virtual_host = bytes_new_from_str(conn->vhost);
    opn->capabilities = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)opn;
    channel_send_frame(conn->chan0, fr1);
    fr1 = NULL;

    // <<< connection_open_ok
    if (channel_expect_method(conn->chan0,
                              AMQP_CONNECTION_OPEN_OK,
                              &fr0) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_RUN + 7;
        goto err;
    }

    mrkthr_sema_release(&conn->chan0->sync_sema);

end:
    amqp_frame_destroy_method(&fr0);
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
    conn->closed = 1;
}


static int
consumer_stop_threads_cb(UNUSED bytes_t *key,
                         amqp_consumer_t *cons,
                         UNUSED void *udata)
{
    cons->closed = 1;
    if (mrkthr_signal_has_owner(&cons->content_sig)) {
        mrkthr_signal_error(&cons->content_sig, MRKAMQP_STOP_THREADS);
    }
    if (cons->content_thread != NULL) {
        int res;
        //CTRACE("joining content thread ...");
        if ((res = mrkthr_join(cons->content_thread)) != 0) {
            //CTRACE("could not see content thread");
        }
        //CTRACE("content thread after join: %p", cons->content_thread);
        //mrkthr_dump(cons->content_thread);
        cons->content_thread = NULL;
    }
    return 0;
}


static int
channel_stop_threads_cb(amqp_channel_t **chan, UNUSED void *udata)
{
    (*chan)->closed = 1;

    (void)hash_traverse(&(*chan)->consumers,
                        (hash_traverser_t)consumer_stop_threads_cb, NULL);

    if (mrkthr_signal_has_owner(&(*chan)->expect_sig)) {
        int res;

        mrkthr_signal_error(&(*chan)->expect_sig, MRKAMQP_STOP_THREADS);
        //CTRACE("joining expect sig owner %p ...", (*chan)->expect_sig.owner);
        if ((res = mrkthr_join((*chan)->expect_sig.owner)) != 0) {
            //CTRACE("could not see expect thread, error: %s", mrkthr_diag_str(res));
        }
        //CTRACE("expect thread after join: %p", (*chan)->expect_sig.owner);
        //mrkthr_dump((*chan)->expect_sig.owner);
        /* expect must have exited by now so finalize for sanity */
        if (mrkthr_signal_has_owner(&(*chan)->expect_sig)) {
            //CTRACE(FRED("expect signal owner is not expected here: %p"), (*chan)->expect_sig.owner);
            //mrkthr_dump_all_ctxes();
        }
        mrkthr_signal_fini(&(*chan)->expect_sig);
    } else {
        //CTRACE("expect sig has no owner, OK");
    }
    return 0;
}


static void
amqp_conn_stop_threads(amqp_conn_t *conn)
{
    if (mrkthr_signal_has_owner(&conn->oframe_sig)) {
        mrkthr_signal_error(&conn->oframe_sig, MRKAMQP_STOP_THREADS);
    }
    if (conn->send_thread != NULL) {
        int res;
        //CTRACE("interrupting and joining send thread %p...", conn->send_thread);
        if ((res = mrkthr_set_interrupt_and_join(conn->send_thread)) != 0) {
            //CTRACE("could not see send thread, error: %s", mrkthr_diag_str(res));
        }
        //CTRACE("send thread after join: %p", conn->send_thread);
        //mrkthr_dump(conn->send_thread);
        conn->send_thread = NULL;
    }
    if (conn->recv_thread != NULL) {
        int res;
        //CTRACE("interrupting and joining recv thread %p...", conn->recv_thread);
        if ((res = mrkthr_set_interrupt_and_join(conn->recv_thread)) != 0) {
            //CTRACE("could not see recv thread, error: %s", mrkthr_diag_str(res));
        }
        //CTRACE("recv thread after join: %p", conn->recv_thread);
        //mrkthr_dump(conn->recv_thread);
    }
    //CTRACE("channels traverse %p", &conn->channels);
    (void)array_traverse(&conn->channels,
                         (array_traverser_t)channel_stop_threads_cb, NULL);
}


static int
close_channel_fast_cb(amqp_channel_t **chan, UNUSED void *udata)
{
    assert(*chan != NULL);
    (void)amqp_close_channel_fast(*chan);
    return 0;
}


static int
close_channel_cb(amqp_channel_t **chan, UNUSED void *udata)
{
    assert(*chan != NULL);
    (void)amqp_close_channel(*chan);
    return 0;
}


int
amqp_conn_close(amqp_conn_t *conn, int flags)
{
    int res;
    amqp_frame_t *fr0, *fr1;
    amqp_connection_close_t *clo;

    assert(conn != NULL);

    res = 0;
    fr0 = NULL;

    if (conn->chan0 == NULL || conn->closed) {
        res = AMQP_CONN_CLOSE + 1;
        goto err;
    }

    /*
     * chan0 cannot be "closed" by AMQP
     */
    conn->chan0->closed = 1;
    //CTRACE("channels traverse %p", &conn->channels);
    if (flags & AMQP_CONN_CLOSE_FFAST) {
        (void)array_traverse(&conn->channels,
                       (array_traverser_t)close_channel_fast_cb, NULL);
    } else {
        (void)array_traverse(&conn->channels,
                       (array_traverser_t)close_channel_cb, NULL);
    }

    if (mrkthr_sema_acquire(&conn->chan0->sync_sema) != 0) {
        res = AMQP_CONN_CLOSE + 2;
        goto err;
    }
    // >>> connection_close
    fr1 = amqp_frame_new(conn->chan0->id, AMQP_FMETHOD);
    clo = NEWREF(connection_close)();
    clo->reply_text = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)clo;
    channel_send_frame(conn->chan0, fr1);
    fr1 = NULL;

    // <<< connection_close_ok
    if (channel_expect_method(conn->chan0,
                              AMQP_CONNECTION_CLOSE_OK,
                              &fr0) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        res = AMQP_CONN_CLOSE + 3;
        goto err;
    }

    mrkthr_sema_release(&conn->chan0->sync_sema);

end:
    amqp_frame_destroy_method(&fr0);
    return res;

err:
    TR(res);
    goto end;
}


void
amqp_conn_post_close(amqp_conn_t *conn)
{
    if (conn->closed) {
        //CTRACE("double amqp_conn_post_close");
        return;
    }

    amqp_conn_stop_threads(conn);
    amqp_conn_close_fd(conn);
}


void
amqp_conn_destroy(amqp_conn_t **conn)
{
    if (*conn != NULL) {
        amqp_frame_t *fr;

        BYTES_DECREF(&(*conn)->error_msg);

        (*conn)->chan0 = NULL;

        amqp_conn_close_fd(*conn); //sanity

        //CTRACE("channels fini %p", &(*conn)->channels);
        array_fini(&(*conn)->channels);
        while ((fr = STQUEUE_HEAD(&(*conn)->oframes)) != NULL) {
            STQUEUE_DEQUEUE(&(*conn)->oframes, link);
            STQUEUE_ENTRY_FINI(link, fr);
            amqp_frame_destroy(&fr);
        }
        STQUEUE_FINI(&(*conn)->oframes);

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

    //CTRACE("channels incr %p", &conn->channels);
    if ((chan = array_incr(&conn->channels)) == NULL) {
        FAIL("array_incr");
    }

    if ((*chan = malloc(sizeof(amqp_channel_t))) == NULL) {
        FAIL("malloc");
    }
    (*chan)->conn = conn;
    (*chan)->id = conn->channels.elnum - 1;
    STQUEUE_INIT(&(*chan)->iframes);
    mrkthr_signal_init(&(*chan)->expect_sig, NULL);
    mrkthr_sema_init(&(*chan)->sync_sema, 1);
    hash_init(&(*chan)->consumers, 17,
              (hash_hashfn_t)bytes_hash,
              (hash_item_comparator_t)bytes_cmp,
              (hash_item_finalizer_t)amqp_consumer_item_fini);
    (*chan)->content_consumer = NULL;
    (*chan)->publish_tag = 0ll;
    STQUEUE_INIT(&(*chan)->pending_pub);
    (*chan)->confirm_mode = 0;
    (*chan)->closed = 1;
    return *chan;
}


static void
channel_send_frame(amqp_channel_t *chan, amqp_frame_t *fr)
{
    STQUEUE_ENQUEUE(&chan->conn->oframes, link, fr);
    mrkthr_signal_send(&chan->conn->oframe_sig);
}


static int
channel_expect_method(amqp_channel_t *chan,
                      amqp_meth_id_t mid,
                      amqp_frame_t **fr)
{
    int res;

    if (mrkthr_signal_has_owner(&chan->expect_sig)) {
        mrkthr_dump(chan->expect_sig.owner);
    }
    assert(!mrkthr_signal_has_owner(&chan->expect_sig));

    mrkthr_signal_init(&chan->expect_sig, mrkthr_me());
    res = 0;

    //CTRACE("subscribing to expect sig ...");
    if (mrkthr_signal_subscribe(&chan->expect_sig) != 0) {
        res = CHANNEL_EXPECT_METHOD + 1;
        TR(res);
        goto err;
    }

    while ((*fr = STQUEUE_HEAD(&chan->iframes)) != NULL) {
        STQUEUE_DEQUEUE(&chan->iframes, link);
        STQUEUE_ENTRY_FINI(link, *fr);

        if ((*fr)->chan != chan->id) {
            TRACEC(FRED("<<< "));
            amqp_frame_dump(*fr);
            TRACEC("\n");
        }
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
                amqp_frame_destroy(fr);
                res = CHANNEL_EXPECT_METHOD + 2;
                goto err;
            }

        } else {
            CTRACE("non-method frame is not expected in the method context");
            amqp_frame_destroy(fr);
            res = CHANNEL_EXPECT_METHOD + 3;
            goto err;
        }
        /*
         * return one frame per call
         */
        break;
    }

end:
    mrkthr_signal_fini(&chan->expect_sig);
    return res;
err:
    goto end;
}


static int
amqp_channel_destroy(amqp_channel_t **chan)
{
    if (*chan != NULL) {
        amqp_frame_t *fr;
        while ((fr = STQUEUE_HEAD(&(*chan)->iframes)) != NULL) {
            STQUEUE_DEQUEUE(&(*chan)->iframes, link);
            STQUEUE_ENTRY_FINI(link, fr);
            amqp_frame_destroy(&fr);
        }
        /* must have been finalized in channel_expect_method() */
        if (mrkthr_signal_has_owner(&(*chan)->expect_sig)) {
            //CTRACE("signal owner has owner %p", (*chan)->expect_sig.owner);
            mrkthr_dump((*chan)->expect_sig.owner);
        }
        assert(!mrkthr_signal_has_owner(&(*chan)->expect_sig));
        hash_fini(&(*chan)->consumers);
        mrkthr_sema_fini(&(*chan)->sync_sema);
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

    assert(conn->chan0 != NULL);

    fr0 = NULL;
    if (mrkthr_sema_acquire(&conn->chan0->sync_sema) != 0) {
        TR(AMQP_CREATE_CHANNEL + 1);
        goto err;
    }
    chan = amqp_channel_new(conn);

    // >>> channel_open
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    opn = NEWREF(channel_open)();
    opn->out_of_band = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)opn;
    channel_send_frame(conn->chan0, fr1);
    fr1 = NULL;

    // <<< channel_open_ok
    if (channel_expect_method(chan, AMQP_CHANNEL_OPEN_OK, &fr0) != 0) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        TR(AMQP_CREATE_CHANNEL + 2);
        goto err;
    }
    if (fr0->chan != chan->id) {
        mrkthr_sema_release(&conn->chan0->sync_sema);
        TR(AMQP_CREATE_CHANNEL + 3);
        goto err;
    }

    chan->closed = 0;
    mrkthr_sema_release(&conn->chan0->sync_sema);

end:
    amqp_frame_destroy_method(&fr0);
    return chan;

err:
    amqp_channel_destroy(&chan);
    goto end;
}


#define AMQP_CHANNEL_METHOD_PAIR(mname,                        \
                                 okmid,                        \
                                 errid,                        \
                                 __a1,                         \
                                 __a0)                         \
    int res;                                                   \
    amqp_frame_t *fr0, *fr1;                                   \
    amqp_##mname##_t *m;                                       \
    fr0 = NULL;                                                \
    if (chan->closed) {                                        \
        res = errid + 1;                                       \
        goto err;                                              \
    }                                                          \
    if (mrkthr_sema_acquire(&chan->sync_sema) != 0) {          \
        res = errid + 2;                                       \
        goto err;                                              \
    }                                                          \
    res = 0;                                                   \
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);              \
    m = NEWREF(mname)();                                       \
    __a1                                                       \
    fr1->payload.params = (amqp_meth_params_t *)m;             \
    channel_send_frame(chan, fr1);                             \
    fr1 = NULL;                                                \
    if (channel_expect_method(chan, okmid, &fr0) != 0) {       \
        res = errid + 3;                                       \
        mrkthr_sema_release(&chan->sync_sema);                 \
        goto err;                                              \
    }                                                          \
    __a0                                                       \
    mrkthr_sema_release(&chan->sync_sema);                     \
end:                                                           \
    amqp_frame_destroy_method(&fr0);                           \
    return res;                                                \
err:                                                           \
    TR(res);                                                   \
    goto end;                                                  \



#define AMQP_CHANNEL_METHOD_PAIR_NOWAIT(mname,                 \
                                        fnowait,               \
                                        okmid,                 \
                                        errid,                 \
                                        __a1,                  \
                                        __a0)                  \
    int res;                                                   \
    amqp_frame_t *fr0, *fr1;                                   \
    amqp_##mname##_t *m;                                       \
    fr0 = NULL;                                                \
    if (chan->closed) {                                        \
        res = errid + 1;                                       \
        goto err;                                              \
    }                                                          \
    if (mrkthr_sema_acquire(&chan->sync_sema) != 0) {          \
        res = errid + 2;                                       \
        goto err;                                              \
    }                                                          \
    res = 0;                                                   \
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);              \
    m = NEWREF(mname)();                                       \
    fr1->payload.params = (amqp_meth_params_t *)m;             \
    __a1                                                       \
    channel_send_frame(chan, fr1);                             \
    fr1 = NULL;                                                \
    if (!(flags & fnowait)) {                                  \
        if (channel_expect_method(chan, okmid, &fr0) != 0) {   \
            res = errid + 3;                                   \
            mrkthr_sema_release(&chan->sync_sema);             \
            goto err;                                          \
        }                                                      \
    }                                                          \
    __a0                                                       \
    mrkthr_sema_release(&chan->sync_sema);                     \
end:                                                           \
    amqp_frame_destroy_method(&fr0);                           \
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
        chan->publish_tag = 0ll;,
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
        m->flags = flags;,
    )
}


int
amqp_channel_declare_exchange_ex(amqp_channel_t *chan,
                                 const char *exchange,
                                 const char *type,
                                 uint8_t flags,
                                 amqp_frame_completion_cb_t cb1,
                                 amqp_frame_completion_cb_t cb0,
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
        if (cb1 != NULL) cb1(chan, fr1, udata);,
        if (cb0 != NULL) cb0(chan, fr0, udata);
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
        m->flags = flags;,
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
        m->flags = flags;,
    )
}


int
amqp_channel_declare_queue_ex(amqp_channel_t *chan,
                              const char *queue,
                              uint8_t flags,
                              amqp_frame_completion_cb_t cb1,
                              amqp_frame_completion_cb_t cb0,
                              void *udata)
{
    AMQP_CHANNEL_METHOD_PAIR_NOWAIT(queue_declare,
                                    DECLARE_QUEUE_FNOWAIT,
                                    AMQP_QUEUE_DECLARE_OK,
                                    AMQP_DECLARE_QUEUE,
        assert(queue != NULL);
        m->queue = bytes_new_from_str(queue);
        m->flags = flags;
        if (cb1 != NULL) cb1(chan, fr1, udata);,
        if (cb0 != NULL) cb0(chan, fr0, udata);
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
        m->flags = flags;,
    )
}


int
amqp_channel_bind_queue_ex(amqp_channel_t *chan,
                           const char *queue,
                           const char *exchange,
                           const char *routing_key,
                           uint8_t flags,
                           amqp_frame_completion_cb_t cb1,
                           amqp_frame_completion_cb_t cb0,
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
        if (cb1 != NULL) cb1(chan, fr1, udata);,
        if (cb0 != NULL) cb0(chan, fr0, udata);
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
        m->routing_key = bytes_new_from_str(routing_key);,
    )
}


int
amqp_channel_unbind_queue_ex(amqp_channel_t *chan,
                             const char *queue,
                             const char *exchange,
                             const char *routing_key,
                             amqp_frame_completion_cb_t cb1,
                             amqp_frame_completion_cb_t cb0,
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
        if (cb1 != NULL) cb1(chan, fr1, udata);,
        if (cb0 != NULL) cb0(chan, fr0, udata);
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
        m->flags = flags;,
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
        m->flags = flags;,
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
        m->flags = flags;,
    )
}


int
amqp_channel_publish(amqp_channel_t *chan,
                     const char *exchange,
                     const char *routing_key,
                     uint8_t flags,
                     amqp_header_completion_cb cb,
                     void *udata,
                     const char *data,
                     ssize_t sz)
{
    int res;
    amqp_frame_t *fr1;
    amqp_basic_publish_t *m;
    amqp_header_t *h;
    amqp_pending_pub_t pp;

    res = 0;

    assert(routing_key != NULL);
    assert(exchange != NULL);

    if (chan->closed) {
        TRRET(CHANNEL_PUBLISH + 1);
    }

    /*
     * 4.2.6 Content Framing: content should be synchronized with
     *   non-content.
     */
    if (mrkthr_sema_acquire(&chan->sync_sema) != 0) {
        TRRET(CHANNEL_PUBLISH + 2);
    }

    STQUEUE_ENTRY_INIT(link, &pp);
    mrkthr_signal_init(&pp.sig, mrkthr_me());
    pp.publish_tag = ++chan->publish_tag;

    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    m = NEWREF(basic_publish)();
    m->exchange = bytes_new_from_str(exchange);
    m->routing_key = bytes_new_from_str(routing_key);
    m->flags = flags;
    fr1->payload.params = (amqp_meth_params_t *)m;
    channel_send_frame(chan, fr1);

    fr1 = amqp_frame_new(chan->id, AMQP_FHEADER);
    h = amqp_header_new();
    h->class_id = AMQP_BASIC;
    h->body_size = sz;
    if (cb != NULL) {
        cb(chan, h, udata);
    }
    fr1->payload.header = h;
    channel_send_frame(chan, fr1);

    while (sz > chan->conn->payload_max) {
        fr1 = amqp_frame_new(chan->id, AMQP_FBODY);
        fr1->sz = chan->conn->payload_max;
        if ((fr1->payload.body = malloc(chan->conn->payload_max)) == NULL) {
            FAIL("malloc");
        }
        memcpy(fr1->payload.body, data, chan->conn->payload_max);
        channel_send_frame(chan, fr1);

        data += chan->conn->payload_max;
        sz -= chan->conn->payload_max;
    }
    if (sz > 0) {
        fr1 = amqp_frame_new(chan->id, AMQP_FBODY);
        fr1->sz = sz;
        if ((fr1->payload.body = malloc(sz)) == NULL) {
            FAIL("malloc");
        }
        memcpy(fr1->payload.body, data, sz);
        channel_send_frame(chan, fr1);
    }
    fr1 = NULL;

    if (chan->confirm_mode) {
        STQUEUE_ENQUEUE(&chan->pending_pub, link, &pp);
        if ((res = mrkthr_signal_subscribe(&pp.sig)) != 0) {
            if (res != MRKAMQP_PROTOCOL_ERROR) {
                res = CHANNEL_PUBLISH + 2;
            }
        }
    }

    mrkthr_signal_fini(&pp.sig);

    mrkthr_sema_release(&chan->sync_sema);
    return res;
}


int
amqp_channel_publish_ex(amqp_channel_t *chan,
                     const char *exchange,
                     const char *routing_key,
                     uint8_t flags,
                     amqp_header_t *header,
                     const char *data)
{
    int res;
    amqp_frame_t *fr1;
    amqp_basic_publish_t *m;
    amqp_pending_pub_t pp;
    ssize_t sz;

    res = 0;

    assert(routing_key != NULL);
    assert(exchange != NULL);

    if (chan->closed) {
        TRRET(CHANNEL_PUBLISH + 3);
    }

    STQUEUE_ENTRY_INIT(link, &pp);
    mrkthr_signal_init(&pp.sig, mrkthr_me());
    pp.publish_tag = ++chan->publish_tag;

    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    m = NEWREF(basic_publish)();
    m->exchange = bytes_new_from_str(exchange);
    m->routing_key = bytes_new_from_str(routing_key);
    m->flags = flags;
    fr1->payload.params = (amqp_meth_params_t *)m;
    channel_send_frame(chan, fr1);

    fr1 = amqp_frame_new(chan->id, AMQP_FHEADER);
    fr1->payload.header = header;
    sz = header->body_size;
    channel_send_frame(chan, fr1);

    while (sz > chan->conn->payload_max) {
        fr1 = amqp_frame_new(chan->id, AMQP_FBODY);
        fr1->sz = chan->conn->payload_max;
        if ((fr1->payload.body = malloc(chan->conn->payload_max)) == NULL) {
            FAIL("malloc");
        }
        memcpy(fr1->payload.body, data, chan->conn->payload_max);
        channel_send_frame(chan, fr1);

        data += chan->conn->payload_max;
        sz -= chan->conn->payload_max;
    }
    if (sz > 0) {
        fr1 = amqp_frame_new(chan->id, AMQP_FBODY);
        fr1->sz = sz;
        if ((fr1->payload.body = malloc(sz)) == NULL) {
            FAIL("malloc");
        }
        memcpy(fr1->payload.body, data, sz);
        channel_send_frame(chan, fr1);
    }
    fr1 = NULL;

    if (chan->confirm_mode) {
        STQUEUE_ENQUEUE(&chan->pending_pub, link, &pp);
        if ((res = mrkthr_signal_subscribe(&pp.sig)) != 0) {
            if (res != MRKAMQP_PROTOCOL_ERROR) {
                res = CHANNEL_PUBLISH + 4;
            }
        }
    }

    mrkthr_signal_fini(&pp.sig);
    return res;
}


/*
 * closing
 */
static int
close_consumer_fast_cb(UNUSED bytes_t *key,
                  amqp_consumer_t*cons,
                  UNUSED void *udata)
{
    assert(cons != NULL);
    amqp_close_consumer_fast(cons);
    return 0;
}


void
amqp_close_channel_fast(amqp_channel_t *chan)
{
    if (chan->closed) {
        return;
    }
    (void)hash_traverse(&chan->consumers,
                        (hash_traverser_t)close_consumer_fast_cb, NULL);
    chan->closed = 1;

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
        BYTES_INCREF(m->consumer_tag);
        m->flags = flags;,
    )
}


static int
close_consumer_cb(UNUSED bytes_t *key,
                  amqp_consumer_t*cons,
                  UNUSED void *udata)
{
    int res;

    assert(cons != NULL);
    if ((res = amqp_close_consumer(cons)) != 0) {
        TR(res);
    }
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

    if (chan->closed) {
        goto end;
    }

    (void)hash_traverse(&chan->consumers,
                        (hash_traverser_t)close_consumer_cb, NULL);

    if (mrkthr_sema_acquire(&chan->sync_sema) != 0) {
        res = AMQP_CLOSE_CHANNEL + 1;
        goto err;
    }

    // >>> channel_close
    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    clo = NEWREF(channel_close)();
    clo->reply_text = bytes_new_from_str("");
    fr1->payload.params = (amqp_meth_params_t *)clo;
    channel_send_frame(chan, fr1);
    fr1 = NULL;

    // <<< channel_close_ok
    if (channel_expect_method(chan, AMQP_CHANNEL_CLOSE_OK, &fr0) != 0) {
        mrkthr_sema_release(&chan->sync_sema);
        res = AMQP_CLOSE_CHANNEL + 2;
        goto err;
    }
    if (fr0->chan != chan->id) {
        mrkthr_sema_release(&chan->sync_sema);
        res = AMQP_CLOSE_CHANNEL + 3;
        goto err;
    }

    mrkthr_sema_release(&chan->sync_sema);

end:
    chan->closed = 1;
    amqp_frame_destroy_method(&fr0);
    return res;

err:
    TR(res);
    goto end;
}


void
amqp_channel_drain_methods(amqp_channel_t *chan)
{
    amqp_frame_t *fr;

    while ((fr = STQUEUE_HEAD(&chan->iframes)) != NULL) {
        STQUEUE_DEQUEUE(&chan->iframes, link);
        STQUEUE_ENTRY_FINI(link, fr);

        assert(fr->chan == chan->id);

        if (fr->type == AMQP_FMETHOD) {
            CTRACE("draining:");
        } else {
            CTRACE("non-method frame is not expected in the method context:");
        }
        amqp_frame_dump(fr);
        TRACEC("\n");
        amqp_frame_destroy(&fr);
    }
}

/*
 * consumer
 */
static amqp_consumer_t *
amqp_consumer_new(amqp_channel_t *chan, uint8_t flags)
{
    amqp_consumer_t *cons;

    if ((cons = malloc(sizeof(amqp_consumer_t))) == NULL) {
        FAIL("malloc");
    }

    cons->chan = chan;
    cons->consumer_tag = NULL;
    mrkthr_signal_init(&cons->content_sig, NULL);
    STQUEUE_INIT(&cons->pending_content);
    cons->content_thread = NULL;
    cons->content_cb = NULL;
    cons->cancel_cb = NULL;
    cons->content_udata = NULL;
    cons->flags = flags;
    cons->closed = 0;

    return cons;
}


static void
amqp_consumer_destroy(amqp_consumer_t **cons)
{
    if (*cons != NULL) {
        amqp_pending_content_t *pc;

        (*cons)->chan = NULL;
        BYTES_DECREF(&(*cons)->consumer_tag);
        assert(!mrkthr_signal_has_owner(&(*cons)->content_sig));
        while ((pc = STQUEUE_HEAD(&(*cons)->pending_content)) != NULL) {
            STQUEUE_DEQUEUE(&(*cons)->pending_content, link);
            STQUEUE_ENTRY_FINI(link, pc);
            amqp_pending_content_destroy(&pc);
        }
        STQUEUE_FINI(&(*cons)->pending_content);
        free(*cons);
        *cons = NULL;
    }
}


static int
amqp_consumer_item_fini(bytes_t *key, amqp_consumer_t *cons)
{
    BYTES_DECREF(&key);
    amqp_consumer_destroy(&cons);
    return 0;
}



amqp_consumer_t *
amqp_channel_create_consumer(amqp_channel_t *chan,
                             const char *queue,
                             const char *consumer_tag,
                             uint8_t flags)
{
    hash_item_t *dit;
    amqp_consumer_t *cons;
    amqp_frame_t *fr0, *fr1;
    amqp_basic_consume_t *m;
    amqp_basic_consume_ok_t *ok;
    bytes_t *ctag;

    fr0 = NULL;
    cons = NULL;

    if ((flags & CONSUME_FNOWAIT) &&
        (consumer_tag == NULL || *consumer_tag == '\0')) {
        TR(CHANNEL_CREATE_CONSUMER + 1);
        goto err;
    }

    if (mrkthr_sema_acquire(&chan->sync_sema) != 0) {
        TR(CHANNEL_CREATE_CONSUMER + 2);
        goto err;
    }

    cons = amqp_consumer_new(chan, flags);

    ctag = bytes_new_from_str( consumer_tag != NULL ? consumer_tag : "");
    BYTES_INCREF(ctag); //nref = 1

    fr1 = amqp_frame_new(chan->id, AMQP_FMETHOD);
    m = NEWREF(basic_consume)();
    assert(queue != NULL);
    m->queue = bytes_new_from_str(queue);
    m->consumer_tag = ctag;
    BYTES_INCREF(m->consumer_tag); //nref = 2
    m->flags = flags;
    fr1->payload.params = (amqp_meth_params_t *)m;
    channel_send_frame(chan, fr1); //nref = 1 (delayed)
    fr1 = NULL;

    if (!(flags & CONSUME_FNOWAIT)) {
        if (channel_expect_method(chan, AMQP_BASIC_CONSUME_OK, &fr0) != 0) {
            mrkthr_sema_release(&chan->sync_sema);
            TR(CHANNEL_CREATE_CONSUMER + 3);
            goto err;
        }

        /* transfer consumer_tag reference from ok to cons */
        ok = (amqp_basic_consume_ok_t *)fr0->payload.params;
        assert(ok->consumer_tag != NULL);
        cons->consumer_tag = ok->consumer_tag;
        ok->consumer_tag = NULL;
    } else {
        cons->consumer_tag = ctag;
        BYTES_INCREF(ctag); //nref = 2
    }

    if ((dit = hash_get_item(&chan->consumers, cons->consumer_tag)) != NULL) {
        mrkthr_sema_release(&chan->sync_sema);
        TR(CHANNEL_CREATE_CONSUMER + 4);
        goto err;
    }
    hash_set_item(&chan->consumers, cons->consumer_tag, cons);
    BYTES_INCREF(cons->consumer_tag); //nref = 3

    mrkthr_sema_release(&chan->sync_sema);

end:
    BYTES_DECREF(&ctag); //nref = 2
    amqp_frame_destroy_method(&fr0);
    return cons;

err:
    amqp_consumer_destroy(&cons);
    goto end;
}


static int
content_thread_worker(UNUSED int argc, void **argv)
{
    int res;
    amqp_consumer_t *cons;

    assert(argc == 1);
    res = 0;
    cons = argv[0];

    if (mrkthr_signal_has_owner(&cons->content_sig)) {
        mrkthr_dump(cons->content_sig.owner);
    }
    assert(!mrkthr_signal_has_owner(&cons->content_sig));
    mrkthr_signal_init(&cons->content_sig, mrkthr_me());

    //CTRACE("consumer %s listening ...", BDATA(cons->consumer_tag));
    while (!cons->closed) {
        amqp_pending_content_t *pc;
        char *data;

        if ((pc = STQUEUE_HEAD(&cons->pending_content)) == NULL) {
            if (mrkthr_signal_subscribe(&cons->content_sig) != 0) {
                res = CONTENT_THREAD_WORKER + 1;
                TR(res);
                goto err;
            }
            continue;
        }

        assert(pc->method != NULL);
        if (pc->method->payload.params->mi->mid == AMQP_BASIC_DELIVER) {
            /*
             * XXX content_cb ?
             */

            if (pc->header == NULL) {
                if (mrkthr_signal_subscribe(&cons->content_sig) != 0) {
                    res = CONTENT_THREAD_WORKER + 2;
                    TR(res);
                    goto err;
                }
                continue;
            }
            /*
             * XXX content_cb ?
             */

            if ((data = malloc(pc->header->payload.header->body_size)) == NULL) {
                FAIL("malloc");
            }

            while (pc->header->payload.header->body_size >
                   pc->header->payload.header->_received_size) {
                size_t sz0;
                amqp_frame_t *fr;

                if ((fr = STQUEUE_HEAD(&pc->body)) == NULL) {
                    if (mrkthr_signal_subscribe(&cons->content_sig) != 0) {
                        res = CONTENT_THREAD_WORKER + 3;
                        TR(res);
                        goto err;
                    }
                    continue;
                }
                sz0 = pc->header->payload.header->_received_size;
                memcpy(data + sz0, fr->payload.body, fr->sz);
                pc->header->payload.header->_received_size += fr->sz;
                STQUEUE_DEQUEUE(&pc->body, link);
                STQUEUE_ENTRY_FINI(link, fr);
                amqp_frame_destroy(&fr);
                /*
                 * XXX content_cb ?
                 */
            }

            assert(cons->content_cb != NULL);
            res = cons->content_cb(pc->method,
                                   pc->header,
                                   data,
                                   cons->content_udata);
            data = NULL; /* passed over to content_cb() */

            if (!(cons->flags & CONSUME_FNOACK)) {
                amqp_frame_t *fr1;
                amqp_basic_ack_t *m;
                amqp_basic_deliver_t *d;

                fr1 = amqp_frame_new(cons->chan->id, AMQP_FMETHOD);
                m = NEWREF(basic_ack)();
                d = (amqp_basic_deliver_t *)pc->method->payload.params;
                m->delivery_tag = d->delivery_tag;
                fr1->payload.params = (amqp_meth_params_t *)m;
                channel_send_frame(cons->chan, fr1);
                fr1 = NULL;
            }

            STQUEUE_DEQUEUE(&cons->pending_content, link);
            STQUEUE_ENTRY_FINI(link, pc);
            amqp_pending_content_destroy(&pc);

            if (res != 0) {
                TR(res);
                break;
            }

        } else if (pc->method->payload.params->mi->mid == AMQP_BASIC_CANCEL) {
            if (cons->cancel_cb != NULL) {
                res = cons->cancel_cb(pc->method,
                                      pc->header,
                                      NULL,
                                      cons->content_udata);
            }

            STQUEUE_DEQUEUE(&cons->pending_content, link);
            STQUEUE_ENTRY_FINI(link, pc);
            amqp_pending_content_destroy(&pc);

            //BYTES_DECREF(&cons->consumer_tag);
            cons->closed = 1;

            if (res != 0) {
                TR(res);
                break;
            }

        } else {
        }
    }

end:
    mrkthr_signal_fini(&cons->content_sig);
    MRKTHRET(res);

err:
    goto end;

}


int
amqp_consumer_handle_content_spawn(amqp_consumer_t *cons,
                                   amqp_consumer_content_cb_t ctcb,
                                   amqp_consumer_content_cb_t clcb,
                                   void *udata)
{
    cons->content_cb = ctcb;
    cons->cancel_cb = clcb;
    cons->content_udata = udata;
    cons->content_thread = mrkthr_spawn((char *)BDATA(cons->consumer_tag),
                                        content_thread_worker,
                                        1,
                                        cons);
    return mrkthr_join(cons->content_thread);

}


int
amqp_consumer_handle_content(amqp_consumer_t *cons,
                             amqp_consumer_content_cb_t ctcb,
                             amqp_consumer_content_cb_t clcb,
                             void *udata)
{
    cons->content_cb = ctcb;
    cons->cancel_cb = clcb;
    cons->content_udata = udata;
    return content_thread_worker(1, (void **)(&cons));
}


void
amqp_close_consumer_fast(amqp_consumer_t *cons)
{
    if (!cons->closed) {
        cons->closed = 1;
        //BYTES_DECREF(&cons->consumer_tag);
    }
}


int
amqp_close_consumer(amqp_consumer_t *cons)
{
    if (!cons->closed) {
        cons->closed = 1;
        assert(cons->consumer_tag != NULL);
        if (amqp_channel_cancel(cons->chan,
                                (const char *)BDATA(cons->consumer_tag),
                                0) != 0) {
            TR(AMQP_CLOSE_CONSUMER + 1);
        }
        //BYTES_DECREF(&cons->consumer_tag);
    }
    return 0;
}

