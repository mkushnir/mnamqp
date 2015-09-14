#ifdef DO_MEMDEBUG
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkamqp_spec);
#endif

#include <mrkcommon/bytestream.h>
#include <mrkcommon/dict.h>
//#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkamqp_private.h>

#include "diag.h"

dict_t methods;


#define FPACK(ty, name) pack_##ty(&conn->outs, m->name)
#define FPACKA(ty, name) pack_##ty(&conn->outs, &m->name)


#define NEW(mname, i, __fields)                                \
NEWDECL(mname)                                                 \
{                                                              \
    amqp_##mname##_t *m;                                       \
    if ((m = malloc(sizeof(amqp_##mname##_t))) == NULL) {      \
        FAIL("malloc");                                        \
    }                                                          \
    m->base.mi = &_methinfo[i];                                \
    __fields                                                   \
    return m;                                                  \
}                                                              \


#define FSTR(n, f)                                             \
    (void)bytestream_nprintf(bs, 1024, #n "=" f " ", m->n)     \


#define FSTRB(n)                                       \
    do {                                               \
        if (m->n != NULL) {                            \
            (void)bytestream_nprintf(bs,               \
                                     1024,             \
                                     #n "='%s' ",      \
                                     m->n->data);      \
        } else {                                       \
            (void)bytestream_nprintf(bs,               \
                                     1024,             \
                                     #n "=%s ",        \
                                     NULL);            \
        }                                              \
    } while (0)                                        \


#define FSTRT(n)                               \
    (void)bytestream_nprintf(bs, 1024, #n "=");\
    table_str(&m->n, bs);                      \
    (void)bytestream_cat(bs, 1, " ")           \



#define STR(mname, __fields)                                   \
void                                                           \
amqp_##mname##_str(UNUSED amqp_meth_params_t *params,          \
                   UNUSED bytestream_t *bs)                    \
{                                                              \
    amqp_##mname##_t *m;                                       \
    m = (amqp_##mname##_t *)params;                            \
    bytestream_nprintf(bs, 1024, "<%s ", m->base.mi->name);    \
    __fields                                                   \
    SADVANCEEOD(bs, -1);                                       \
    bytestream_cat(bs, 2, ">");                                \
}                                                              \


#define ENC(mname, __fields)                           \
int                                                    \
amqp_##mname##_enc(UNUSED amqp_meth_params_t *self,    \
                   UNUSED amqp_conn_t *conn)           \
{                                                      \
    UNUSED amqp_##mname##_t *m;                        \
    m = (amqp_##mname##_t *)self;                      \
    __fields                                           \
    return 0;                                          \
}                                                      \


#define FUNPACK(ty, name)                              \
if (unpack_##ty(&conn->ins, conn->fd, &m->name) < 0) { \
    TRRET(UNPACK + 100);                               \
}                                                      \


#define DEC(mname, __fields)                                   \
int                                                            \
amqp_##mname##_dec(UNUSED amqp_conn_t *conn,                   \
                   UNUSED amqp_meth_params_t **p)              \
{                                                              \
    amqp_##mname##_t *m;                                       \
    if ((m = malloc(sizeof(amqp_##mname##_t))) == NULL) {      \
        FAIL("malloc");                                        \
    }                                                          \
    *p = (amqp_meth_params_t *)m;                              \
    __fields                                                   \
    return 0;                                                  \
}                                                              \


#define FINI(mname, __fields)                          \
void                                                   \
amqp_##mname##_fini(UNUSED amqp_meth_params_t *params) \
{                                                      \
    UNUSED amqp_##mname##_t *m;                        \
    m = (amqp_##mname##_t *)params;                    \
    __fields                                           \
}                                                      \

static amqp_method_info_t _methinfo[];


/*
 * connection.start
 */
NEW(connection_start, 0,
    m->version_major = 0;
    m->version_minor = 0;
    init_table(&m->server_properties);
    m->mechanisms = NULL;
    m->locales = NULL;
)
STR(connection_start,
    FSTR(version_major, "%hhd");
    FSTR(version_minor, "%hhd");
    FSTRT(server_properties);
    FSTRB(mechanisms);
    FSTRB(locales);
)
ENC(connection_start,
    FPACK(octet, version_major);
    FPACK(octet, version_minor);
    FPACKA(table, server_properties);
    FPACK(longstr, mechanisms);
    FPACK(longstr, locales);
)
DEC(connection_start,
    FUNPACK(octet, version_major);
    FUNPACK(octet, version_minor);
    FUNPACK(table, server_properties);
    FUNPACK(longstr, mechanisms);
    FUNPACK(longstr, locales);
)
FINI(connection_start,
    dict_fini(&m->server_properties);
    BYTES_DECREF(&m->mechanisms);
    BYTES_DECREF(&m->locales);
)


/*
 * connection.start_ok
 */
NEW(connection_start_ok, 1,
    init_table(&m->client_properties);
    m->mechanism = NULL;
    m->response = NULL;
    m->locale = NULL;
)
STR(connection_start_ok,
    FSTRT(client_properties);
    FSTRB(mechanism);
    FSTRB(response);
    FSTRB(locale);
)
ENC(connection_start_ok,
    FPACKA(table, client_properties);
    FPACK(shortstr, mechanism);
    FPACK(longstr, response);
    FPACK(shortstr, locale);
)
DEC(connection_start_ok,
    FUNPACK(table, client_properties);
    FUNPACK(shortstr, mechanism);
    FUNPACK(longstr, response);
    FUNPACK(shortstr, locale);
)
FINI(connection_start_ok,
    dict_fini(&m->client_properties);
    BYTES_DECREF(&m->mechanism);
    BYTES_DECREF(&m->response);
    BYTES_DECREF(&m->locale);
)


/*
 * connection.secure
 */
NEW(connection_secure, 2,
    m->challenge = NULL;
)
STR(connection_secure,
    FSTRB(challenge);
)
ENC(connection_secure,
    FPACK(longstr, challenge);
)
DEC(connection_secure,
    FUNPACK(longstr, challenge);
)
FINI(connection_secure,
    BYTES_DECREF(&m->challenge);
)


/*
 * connection.secure_ok
 */
NEW(connection_secure_ok, 3,
    m->response = NULL;
)
STR(connection_secure_ok,
    FSTRB(response);
)
ENC(connection_secure_ok,
    FPACK(longstr, response);
)
DEC(connection_secure_ok,
    FUNPACK(longstr, response);
)
FINI(connection_secure_ok,
    BYTES_DECREF(&m->response);
)


/*
 * connection.tune
 */
NEW(connection_tune, 4,
    m->channel_max = 0;
    m->frame_max = 0;
    m->heartbeat = 0;
)
STR(connection_tune,
    FSTR(channel_max, "%hd");
    FSTR(frame_max, "%ld");
    FSTR(heartbeat, "%hd");
)
ENC(connection_tune,
    FPACK(short, channel_max);
    FPACK(long, frame_max);
    FPACK(short, heartbeat);
)
DEC(connection_tune,
    FUNPACK(short, channel_max);
    FUNPACK(long, frame_max);
    FUNPACK(short, heartbeat);
)
FINI(connection_tune,
)


/*
 * connection.tune_ok
 */
NEW(connection_tune_ok, 5,
    m->channel_max = 0;
    m->frame_max = 0;
    m->heartbeat = 0;
)
STR(connection_tune_ok,
    FSTR(channel_max, "%hd");
    FSTR(frame_max, "%ld");
    FSTR(heartbeat, "%hd");
)
ENC(connection_tune_ok,
    FPACK(short, channel_max);
    FPACK(long, frame_max);
    FPACK(short, heartbeat);
)
DEC(connection_tune_ok,
    FUNPACK(short, channel_max);
    FUNPACK(long, frame_max);
    FUNPACK(short, heartbeat);
)
FINI(connection_tune_ok,
)


/*
 * connection.open
 */
NEW(connection_open, 6,
    m->virtual_host = NULL;
    m->capabilities = NULL;
    m->flags = 0;
)
STR(connection_open,
    FSTRB(virtual_host);
    FSTRB(capabilities);
    FSTR(flags, "%02hhx");
)
ENC(connection_open,
    FPACK(shortstr, virtual_host);
    FPACK(shortstr, capabilities);
    FPACK(octet, flags);
)
DEC(connection_open,
    FUNPACK(shortstr, virtual_host);
    FUNPACK(shortstr, capabilities);
    FUNPACK(octet, flags);
)
FINI(connection_open,
    BYTES_DECREF(&m->virtual_host);
    BYTES_DECREF(&m->capabilities);
)


/*
 * connection.open_ok
 */
NEW(connection_open_ok, 7,
    m->known_hosts = NULL;
)
STR(connection_open_ok,
    FSTRB(known_hosts);
)
ENC(connection_open_ok,
    FPACK(shortstr, known_hosts);
)
DEC(connection_open_ok,
    FUNPACK(shortstr, known_hosts);
)
FINI(connection_open_ok,
    BYTES_DECREF(&m->known_hosts);
)


/*
 * connection.close
 */
NEW(connection_close, 8,
    m->reply_code = 0;
    m->reply_text=  NULL;
    m->class_id = 0;
    m->method_id = 0;
)
STR(connection_close,
    FSTR(reply_code, "%hd");
    FSTRB(reply_text);
    FSTR(class_id, "%hd");
    FSTR(method_id, "%hd");
)
ENC(connection_close,
    FPACK(short, reply_code);
    FPACK(shortstr, reply_text);
    FPACK(short, class_id);
    FPACK(short, method_id);
)
DEC(connection_close,
    FUNPACK(short, reply_code);
    FUNPACK(shortstr, reply_text);
    FUNPACK(short, class_id);
    FUNPACK(short, method_id);
)
FINI(connection_close,
    BYTES_DECREF(&m->reply_text);
)


/*
 * connection.close_ok
 */
NEW(connection_close_ok, 9,
)
STR(connection_close_ok,
)
ENC(connection_close_ok,
)
DEC(connection_close_ok,
)
FINI(connection_close_ok,
)


/*
 * channel.open
 */
NEW(channel_open, 10,
    m->out_of_band = NULL;
)
STR(channel_open,
    FSTRB(out_of_band);
)
ENC(channel_open,
    FPACK(shortstr, out_of_band);
)
DEC(channel_open,
    FUNPACK(shortstr, out_of_band);
)
FINI(channel_open,
    BYTES_DECREF(&m->out_of_band);
)


/*
 * channel.open_ok
 */
NEW(channel_open_ok, 11,
    m->channel_id = 0;
)
STR(channel_open_ok,
    FSTR(channel_id, "%ld");
)
ENC(channel_open_ok,
    FPACK(long, channel_id);
)
DEC(channel_open_ok,
    FUNPACK(long, channel_id);
)
FINI(channel_open_ok,
)


/*
 * channel.flow
 */
NEW(channel_flow, 12,
    m->flags = 0;
)
STR(channel_flow,
    FSTR(flags, "%02hhx");
)
ENC(channel_flow,
    FPACK(octet, flags);
)
DEC(channel_flow,
    FUNPACK(octet, flags);
)
FINI(channel_flow,
)


/*
 * channel.flow_ok
 */
NEW(channel_flow_ok, 13,
    m->flags = 0;
)
STR(channel_flow_ok,
    FSTR(flags, "%02hhx");
)
ENC(channel_flow_ok,
    FPACK(octet, flags);
)
DEC(channel_flow_ok,
    FUNPACK(octet, flags);
)
FINI(channel_flow_ok,
)


/*
 * channel.close
 */
NEW(channel_close, 14,
    m->reply_code = 0;
    m->reply_text=  NULL;
    m->class_id = 0;
    m->method_id = 0;
)
STR(channel_close,
    FSTR(reply_code, "%hd");
    FSTRB(reply_text);
    FSTR(class_id, "%hd");
    FSTR(method_id, "%hd");
)
ENC(channel_close,
    FPACK(short, reply_code);
    FPACK(shortstr, reply_text);
    FPACK(short, class_id);
    FPACK(short, method_id);
)
DEC(channel_close,
    FUNPACK(short, reply_code);
    FUNPACK(shortstr, reply_text);
    FUNPACK(short, class_id);
    FUNPACK(short, method_id);
)
FINI(channel_close,
    BYTES_DECREF(&m->reply_text);
)


/*
 * channel.close_ok
 */
NEW(channel_close_ok, 15,
)
STR(channel_close_ok,
)
ENC(channel_close_ok,
)
DEC(channel_close_ok,
)
FINI(channel_close_ok,
)


/*
 * confirm.select
 */
NEW(confirm_select, 16,
    m->flags = 0;
)
STR(confirm_select,
    FSTR(flags, "%02hhx");
)
ENC(confirm_select,
    FPACK(octet, flags);
)
DEC(confirm_select,
    FUNPACK(octet, flags);
)
FINI(confirm_select,
)


/*
 * confirm.select_ok
 */
NEW(confirm_select_ok, 17,
)
STR(confirm_select_ok,
)
ENC(confirm_select_ok,
)
DEC(confirm_select_ok,
)
FINI(confirm_select_ok,
)





/*
 * exchange.declare
 */
NEW(exchange_declare, 18,
    m->ticket = 0;
    m->exchange = NULL;
    m->type = NULL;
    m->flags = 0;
    init_table(&m->arguments);
)
STR(exchange_declare,
    FSTR(ticket, "%hd");
    FSTRB(exchange);
    FSTRB(type);
    FSTR(flags, "%02hhx");
    FSTRT(arguments);
)
ENC(exchange_declare,
    FPACK(short, ticket);
    FPACK(shortstr, exchange);
    FPACK(shortstr, type);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)
DEC(exchange_declare,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, type);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)
FINI(exchange_declare,
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->type);
    dict_fini(&m->arguments);
)


/*
 * exchange.declare_ok
 */
NEW(exchange_declare_ok, 19,
)
STR(exchange_declare_ok,
)
ENC(exchange_declare_ok,
)
DEC(exchange_declare_ok,
)
FINI(exchange_declare_ok,
)


/*
 * exchange.delete
 */
NEW(exchange_delete, 20,
    m->ticket = 0;
    m->exchange = NULL;
    m->flags = 0;
)
STR(exchange_delete,
    FSTR(ticket, "%hd");
    FSTRB(exchange);
    FSTR(flags, "%02hhx");
)
ENC(exchange_delete,
    FPACK(short, ticket);
    FPACK(shortstr, exchange);
    FPACK(octet, flags);
)
DEC(exchange_delete,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, exchange);
    FUNPACK(octet, flags);
)
FINI(exchange_delete,
    BYTES_DECREF(&m->exchange);
)


/*
 * exchange.delete_ok
 */
NEW(exchange_delete_ok, 21,
)
STR(exchange_delete_ok,
)
ENC(exchange_delete_ok,
)
DEC(exchange_delete_ok,
)
FINI(exchange_delete_ok,
)




/*
 * queue.declare
 */
NEW(queue_declare, 22,
    m->ticket = 0;
    m->queue = NULL;
    m->flags = 0;
    init_table(&m->arguments);
)
STR(queue_declare,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTR(flags, "%02hhx");
    FSTRT(arguments);
)
ENC(queue_declare,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)
DEC(queue_declare,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)
FINI(queue_declare,
    BYTES_DECREF(&m->queue);
    dict_fini(&m->arguments);
)


/*
 * queue.declare_ok
 */
NEW(queue_declare_ok, 23,
    m->queue = NULL;
    m->message_count = 0;
    m->consumer_count = 0;
)
STR(queue_declare_ok,
    FSTRB(queue);
    FSTR(message_count, "%d");
    FSTR(consumer_count, "%d");
)
ENC(queue_declare_ok,
    FPACK(shortstr, queue);
    FPACK(long, message_count);
    FPACK(long, consumer_count);
)
DEC(queue_declare_ok,
    FUNPACK(shortstr, queue);
    FUNPACK(long, message_count);
    FUNPACK(long, consumer_count);
)
FINI(queue_declare_ok,
    BYTES_DECREF(&m->queue);
)


/*
 * queue.bind
 */
NEW(queue_bind, 24,
    m->ticket = 0;
    m->queue = NULL;
    m->exchange = NULL;
    m->routing_key = NULL;
    m->flags = 0;
    init_table(&m->arguments);
)
STR(queue_bind,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTRB(exchange);
    FSTRB(routing_key);
    FSTR(flags, "%02hhx");
    FSTRT(arguments);
)
ENC(queue_bind,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)
DEC(queue_bind,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, routing_key);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)
FINI(queue_bind,
    BYTES_DECREF(&m->queue);
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
    dict_fini(&m->arguments);
)


/*
 * queue.bind_ok
 */
NEW(queue_bind_ok, 25,
)
STR(queue_bind_ok,
)
ENC(queue_bind_ok,
)
DEC(queue_bind_ok,
)
FINI(queue_bind_ok,
)


/*
 * queue.purge
 */
NEW(queue_purge, 26,
    m->ticket = 0;
    m->queue = NULL;
    m->flags = 0;
)
STR(queue_purge,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTR(flags, "%02hhx");
)
ENC(queue_purge,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(octet, flags);
)
DEC(queue_purge,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(octet, flags);
)
FINI(queue_purge,
    BYTES_DECREF(&m->queue);
)


/*
 * queue.purge_ok
 */
NEW(queue_purge_ok, 27,
    m->message_count = 0;
)
STR(queue_purge_ok,
    FSTR(message_count, "%d");
)
ENC(queue_purge_ok,
    FPACK(long, message_count);
)
DEC(queue_purge_ok,
    FUNPACK(long, message_count);
)
FINI(queue_purge_ok,
)


/*
 * queue.delete
 */
NEW(queue_delete, 28,
    m->ticket = 0;
    m->queue = NULL;
    m->flags = 0;
)
STR(queue_delete,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTR(flags, "%02hhx");
)
ENC(queue_delete,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(octet, flags);
)
DEC(queue_delete,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(octet, flags);
)
FINI(queue_delete,
    BYTES_DECREF(&m->queue);
)


/*
 * queue.delete_ok
 */
NEW(queue_delete_ok, 29,
    m->message_count = 0;
)
STR(queue_delete_ok,
    FSTR(message_count, "%d");
)
ENC(queue_delete_ok,
    FPACK(long, message_count);
)
DEC(queue_delete_ok,
    FUNPACK(long, message_count);
)
FINI(queue_delete_ok,
)


/*
 * queue.unbind
 */
NEW(queue_unbind, 30,
    m->ticket = 0;
    m->queue = NULL;
    m->exchange = NULL;
    m->routing_key = NULL;
    init_table(&m->arguments);
)
STR(queue_unbind,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTRB(exchange);
    FSTRB(routing_key);
    FSTRT(arguments);
)
ENC(queue_unbind,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
    FPACKA(table, arguments);
)
DEC(queue_unbind,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, routing_key);
    FUNPACK(table, arguments);
)
FINI(queue_unbind,
    BYTES_DECREF(&m->queue);
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
    dict_fini(&m->arguments);
)


/*
 * queue.unbind_ok
 */
NEW(queue_unbind_ok, 31,
)
STR(queue_unbind_ok,
)
ENC(queue_unbind_ok,
)
DEC(queue_unbind_ok,
)
FINI(queue_unbind_ok,
)


/*
 * basic.qos
 */
NEW(basic_qos, 32,
    m->prefetch_size = 0;
    m->prefetch_count = 0;
    m->flags = 0;
)
STR(basic_qos,
    FSTR(prefetch_size, "%d");
    FSTR(prefetch_count, "%hd");
    FSTR(flags, "%02hhx");
)
ENC(basic_qos,
    FPACK(long, prefetch_size);
    FPACK(short, prefetch_count);
    FPACK(octet, flags);
)
DEC(basic_qos,
    FUNPACK(long, prefetch_size);
    FUNPACK(short, prefetch_count);
    FUNPACK(octet, flags);
)
FINI(basic_qos,
)


/*
 * basic.qos_ok
 */
NEW(basic_qos_ok, 33,
)
STR(basic_qos_ok,
)
ENC(basic_qos_ok,
)
DEC(basic_qos_ok,
)
FINI(basic_qos_ok,
)


/*
 * basic.consume
 */
NEW(basic_consume, 34,
    m->ticket = 0;
    m->queue = NULL;
    m->consumer_tag = NULL;
    m->flags = 0;
    init_table(&m->arguments);
)
STR(basic_consume,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTRB(consumer_tag);
    FSTR(flags, "%02hhx");
    FSTRT(arguments);
)
ENC(basic_consume,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(shortstr, consumer_tag);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)
DEC(basic_consume,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(shortstr, consumer_tag);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)
FINI(basic_consume,
    BYTES_DECREF(&m->queue);
    BYTES_DECREF(&m->consumer_tag);
    dict_fini(&m->arguments);
)


/*
 * basic.consume_ok
 */
NEW(basic_consume_ok, 35,
    m->consumer_tag = NULL;
)
STR(basic_consume_ok,
    FSTRB(consumer_tag);
)
ENC(basic_consume_ok,
    FPACK(shortstr, consumer_tag);
)
DEC(basic_consume_ok,
    FUNPACK(shortstr, consumer_tag);
)
FINI(basic_consume_ok,
    BYTES_DECREF(&m->consumer_tag);
)


/*
 * basic.cancel
 */
NEW(basic_cancel, 36,
    m->consumer_tag = NULL;
    m->flags = 0;
)
STR(basic_cancel,
    FSTRB(consumer_tag);
    FSTR(flags, "%02hhx");
)
ENC(basic_cancel,
    FPACK(shortstr, consumer_tag);
    FPACK(octet, flags);
)
DEC(basic_cancel,
    FUNPACK(shortstr, consumer_tag);
    FUNPACK(octet, flags);
)
FINI(basic_cancel,
    BYTES_DECREF(&m->consumer_tag);
)


/*
 * basic.cancel_ok
 */
NEW(basic_cancel_ok, 37,
    m->consumer_tag = NULL;
)
STR(basic_cancel_ok,
    FSTRB(consumer_tag);
)
ENC(basic_cancel_ok,
    FPACK(shortstr, consumer_tag);
)
DEC(basic_cancel_ok,
    FUNPACK(shortstr, consumer_tag);
)
FINI(basic_cancel_ok,
    BYTES_DECREF(&m->consumer_tag);
)


/*
 * basic.publish
 */
NEW(basic_publish, 38,
    m->ticket = 0;
    m->exchange = NULL;
    m->routing_key = NULL;
    m->flags = 0;
)
STR(basic_publish,
    FSTR(ticket, "%hd");
    FSTRB(exchange);
    FSTRB(routing_key);
    FSTR(flags, "%02hhx");
)
ENC(basic_publish,
    FPACK(short, ticket);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
    FPACK(octet, flags);
)
DEC(basic_publish,
)
FINI(basic_publish,
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
)


/*
 * basic.return
 */
NEW(basic_return, 39,
    m->reply_code = 0;
    m->reply_text = NULL;
    m->exchange = NULL;
    m->routing_key = NULL;
)
STR(basic_return,
    FSTR(reply_code, "%hd");
    FSTRB(reply_text);
    FSTRB(exchange);
    FSTRB(routing_key);
)
ENC(basic_return,
    FPACK(short, reply_code);
    FPACK(shortstr, reply_text);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
)
DEC(basic_return,
    FUNPACK(short, reply_code);
    FUNPACK(shortstr, reply_text);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, routing_key);
)
FINI(basic_return,
    BYTES_DECREF(&m->reply_text);
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
)


/*
 * basic.deliver
 */
NEW(basic_deliver, 40,
    m->consumer_tag = NULL;
    m->delivery_tag = 0;
    m->flags = 0;
    m->exchange = NULL;
    m->routing_key = NULL;
)
STR(basic_deliver,
    FSTRB(consumer_tag);
    FSTR(delivery_tag, "%016lx");
    FSTR(flags, "%02hhx");
    FSTRB(exchange);
    FSTRB(routing_key);
)
ENC(basic_deliver,
    FPACK(shortstr, consumer_tag);
    FPACK(longlong, delivery_tag);
    FPACK(octet, flags);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
)
DEC(basic_deliver,
    FUNPACK(shortstr, consumer_tag);
    FUNPACK(longlong, delivery_tag);
    FUNPACK(octet, flags);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, routing_key);
)
FINI(basic_deliver,
    BYTES_DECREF(&m->consumer_tag);
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
)


/*
 * basic.get
 */
NEW(basic_get, 41,
    m->ticket = 0;
    m->queue = NULL;
    m->flags = 0;
)
STR(basic_get,
    FSTR(ticket, "%hd");
    FSTRB(queue);
    FSTR(flags, "%02hhx");
)
ENC(basic_get,
    FPACK(short, ticket);
    FPACK(shortstr, queue);
    FPACK(octet, flags);
)
DEC(basic_get,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, queue);
    FUNPACK(octet, flags);
)
FINI(basic_get,
    BYTES_DECREF(&m->queue);
)


/*
 * basic.get_ok
 */
NEW(basic_get_ok, 42,
    m->delivery_tag = 0;
    m->flags = 0;
    m->exchange = NULL;
    m->routing_key = NULL;
    m->message_count = 0;
)
STR(basic_get_ok,
    FSTR(delivery_tag, "016lx");
    FSTR(flags, "%02hhx");
    FSTRB(exchange);
    FSTRB(routing_key);
    FSTR(message_count, "%d");
)
ENC(basic_get_ok,
    FPACK(longlong, delivery_tag);
    FPACK(octet, flags);
    FPACK(shortstr, exchange);
    FPACK(shortstr, routing_key);
    FPACK(long, message_count);
)
DEC(basic_get_ok,
    FUNPACK(longlong, delivery_tag);
    FUNPACK(octet, flags);
    FUNPACK(shortstr, exchange);
    FUNPACK(shortstr, routing_key);
    FUNPACK(long, message_count);
)
FINI(basic_get_ok,
    BYTES_DECREF(&m->exchange);
    BYTES_DECREF(&m->routing_key);
)


/*
 * basic.get_empty
 */
NEW(basic_get_empty, 43,
    m->cluster_id = NULL;
)
STR(basic_get_empty,
    FSTRB(cluster_id);
)
ENC(basic_get_empty,
    FPACK(shortstr, cluster_id);
)
DEC(basic_get_empty,
    FUNPACK(shortstr, cluster_id);
)
FINI(basic_get_empty,
    BYTES_DECREF(&m->cluster_id);
)


/*
 * basic.ack
 */
NEW(basic_ack, 44,
    m->delivery_tag = 0;
    m->flags = 0;
)
STR(basic_ack,
    FSTR(delivery_tag, "%016lx");
    FSTR(flags, "%02hhx");
)
ENC(basic_ack,
    FPACK(longlong, delivery_tag);
    FPACK(octet, flags);
)
DEC(basic_ack,
    FUNPACK(longlong, delivery_tag);
    FUNPACK(octet, flags);
)
FINI(basic_ack,
)


/*
 * basic.reject
 */
NEW(basic_reject, 45,
    m->delivery_tag = 0;
    m->flags = 0;
)
STR(basic_reject,
    FSTR(delivery_tag, "%016lx");
    FSTR(flags, "%02hhx");
)
ENC(basic_reject,
    FPACK(longlong, delivery_tag);
    FPACK(octet, flags);
)
DEC(basic_reject,
    FUNPACK(longlong, delivery_tag);
    FUNPACK(octet, flags);
)
FINI(basic_reject,
)


/*
 * basic.recover_async
 */
NEW(basic_recover_async, 46,
    m->flags = 0;
)
STR(basic_recover_async,
    FSTR(flags, "%02hhx");
)
ENC(basic_recover_async,
    FPACK(octet, flags);
)
DEC(basic_recover_async,
    FUNPACK(octet, flags);
)
FINI(basic_recover_async,
)


/*
 * basic.recover
 */
NEW(basic_recover, 47,
    m->flags = 0;
)
STR(basic_recover,
    FSTR(flags, "%02hhx");
)
ENC(basic_recover,
    FPACK(octet, flags);
)
DEC(basic_recover,
    FUNPACK(octet, flags);
)
FINI(basic_recover,
)


/*
 * basic.recover_ok
 */
NEW(basic_recover_ok, 48,
)
STR(basic_recover_ok,
)
ENC(basic_recover_ok,
)
DEC(basic_recover_ok,
)
FINI(basic_recover_ok,
)


/*
 * basic.nack
 */
NEW(basic_nack, 49,
    m->delivery_tag = 0;
    m->flags = 0;
)
STR(basic_nack,
    FSTR(delivery_tag, "%016lx");
    FSTR(flags, "%02hhx");
)
ENC(basic_nack,
    FPACK(longlong, delivery_tag);
    FPACK(octet, flags);
)
DEC(basic_nack,
    FUNPACK(longlong, delivery_tag);
    FUNPACK(octet, flags);
)
FINI(basic_nack,
)


/*
 * registry
 */
#define MI(id, cls, meth)                              \
{                                                      \
    #cls "." #meth,                                    \
    id,                                                \
    (amqp_method_new_t)amqp_##cls##_##meth##_new,      \
    amqp_##cls##_##meth##_str,                         \
    amqp_##cls##_##meth##_enc,                         \
    amqp_##cls##_##meth##_dec,                         \
    amqp_##cls##_##meth##_fini                         \
}                                                      \


static amqp_method_info_t _methinfo[] = {
    MI(AMQP_CONNECTION_START, connection, start),
    MI(AMQP_CONNECTION_START_OK, connection, start_ok),
    MI(AMQP_CONNECTION_SECURE, connection, secure),
    MI(AMQP_CONNECTION_SECURE_OK, connection, secure_ok),
    MI(AMQP_CONNECTION_TUNE, connection, tune),
    MI(AMQP_CONNECTION_TUNE_OK, connection, tune_ok),
    MI(AMQP_CONNECTION_OPEN, connection, open),
    MI(AMQP_CONNECTION_OPEN_OK, connection, open_ok),
    MI(AMQP_CONNECTION_CLOSE, connection, close),
    MI(AMQP_CONNECTION_CLOSE_OK, connection, close_ok),
    /* 9 */

    MI(AMQP_CHANNEL_OPEN, channel, open),
    MI(AMQP_CHANNEL_OPEN_OK, channel, open_ok),
    MI(AMQP_CHANNEL_FLOW, channel, flow),
    MI(AMQP_CHANNEL_FLOW_OK, channel, flow_ok),
    MI(AMQP_CHANNEL_CLOSE, channel, close),
    MI(AMQP_CHANNEL_CLOSE_OK, channel, close_ok),
    /* 15 */

    MI(AMQP_CONFIRM_SELECT, confirm, select),
    MI(AMQP_CONFIRM_SELECT_OK, confirm, select_ok),
    /* 17 */

    MI(AMQP_EXCHANGE_DECLARE, exchange, declare),
    MI(AMQP_EXCHANGE_DECLARE_OK, exchange, declare_ok),
    MI(AMQP_EXCHANGE_DELETE, exchange, delete),
    MI(AMQP_EXCHANGE_DELETE_OK, exchange, delete_ok),
    /* 21 */

    MI(AMQP_QUEUE_DECLARE, queue, declare),
    MI(AMQP_QUEUE_DECLARE_OK, queue, declare_ok),
    MI(AMQP_QUEUE_BIND, queue, bind),
    MI(AMQP_QUEUE_BIND_OK, queue, bind_ok),
    MI(AMQP_QUEUE_PURGE, queue, purge),
    MI(AMQP_QUEUE_PURGE_OK, queue, purge_ok),
    MI(AMQP_QUEUE_DELETE, queue, delete),
    MI(AMQP_QUEUE_DELETE_OK, queue, delete_ok),
    MI(AMQP_QUEUE_UNBIND, queue, unbind),
    MI(AMQP_QUEUE_UNBIND_OK, queue, unbind_ok),
    /* 31 */

    MI(AMQP_BASIC_QOS, basic, qos),
    MI(AMQP_BASIC_QOS_OK, basic, qos_ok),
    MI(AMQP_BASIC_CONSUME, basic, consume),
    MI(AMQP_BASIC_CONSUME_OK, basic, consume_ok),
    MI(AMQP_BASIC_CANCEL, basic, cancel),
    MI(AMQP_BASIC_CANCEL_OK, basic, cancel_ok),
    MI(AMQP_BASIC_PUBLISH, basic, publish),
    MI(AMQP_BASIC_RETURN, basic, return),
    MI(AMQP_BASIC_DELIVER, basic, deliver),
    MI(AMQP_BASIC_GET, basic, get),
    MI(AMQP_BASIC_GET_OK, basic, get_ok),
    MI(AMQP_BASIC_GET_EMPTY, basic, get_empty),
    MI(AMQP_BASIC_ACK, basic, ack),
    MI(AMQP_BASIC_REJECT, basic, reject),
    MI(AMQP_BASIC_RECOVER_ASYNC, basic, recover_async),
    MI(AMQP_BASIC_RECOVER, basic, recover),
    MI(AMQP_BASIC_RECOVER_OK, basic, recover_ok),
    MI(AMQP_BASIC_NACK, basic, nack),
    /* 49 */

};


amqp_method_info_t *
amqp_method_info_get(amqp_meth_id_t mid)
{
    dict_item_t *dit;
    amqp_method_info_t *mi;

    mi = NULL;
    if ((dit = dict_get_item(&methods, (void *)(uintptr_t)mid)) != NULL) {
        mi = dit->value;
    }
    return mi;
}


int
amqp_meth_params_decode(amqp_conn_t *conn,
                        amqp_meth_id_t mid,
                        amqp_meth_params_t **params)
{
    int res;
    dict_item_t *dit;
    amqp_method_info_t *mi;

    if ((dit = dict_get_item(&methods, (void *)(uintptr_t)mid)) == NULL) {
        TRACE("invalid mid %016lx", mid);
        TRRET(AMQP_METH_PARAMS_DECODE + 1);
    }
    mi = dit->value;
    assert(mi->mid == mid);
    res =  mi->dec(conn, params);
    (*params)->mi = mi;
    return res;
}


void
amqp_meth_params_dump(amqp_meth_params_t *params)
{
    bytestream_t bs;

    bytestream_init(&bs, 1024);
    params->mi->str(params, &bs);
    TRACEC("%s", SDATA(&bs, 0));
    bytestream_fini(&bs);
}

void
amqp_meth_params_destroy(amqp_meth_params_t **params)
{
    if (*params != NULL) {
        assert((*params)->mi != NULL);
        (*params)->mi->fini(*params);
        free(*params);
        *params = NULL;
    }
}


static uint64_t
method_info_hash(amqp_meth_id_t mid)
{
    return mid;
}

static int
method_info_cmp(amqp_meth_id_t a, amqp_meth_id_t b)
{
    return (int)(int64_t)(a - b);
}


/*
 * header
 */

amqp_header_t *
amqp_header_new(void)
{
    amqp_header_t *header;

    if ((header=  malloc(sizeof(amqp_header_t))) == NULL) {
        FAIL("malloc");
    }
    header->class_id = 0;
    header->weight = 0;
    header->body_size = 0ll;
    header->flags = 0;

    header->content_type = NULL;
    header->content_encoding = NULL;
    init_table(&header->headers);
    header->delivery_mode = 0;
    header->priority = 0;
    header->correlation_id = NULL;
    header->reply_to = NULL;
    header->expiration = NULL;
    header->message_id = NULL;
    header->timestamp = 0;
    header->type = NULL;
    header->user_id = NULL;
    header->app_id = NULL;
    header->cluster_id = NULL;

    header->_received_size = 0;
    return header;
}


#define HFSTR(fl, n, f) if (m->flags & AMQP_HEADER_F##fl) FSTR(n, f)
#define HFSTRB(fl, n) if (m->flags & AMQP_HEADER_F##fl) FSTRB(n)
#define HFSTRT(fl, n) if (m->flags & AMQP_HEADER_F##fl) FSTRT(n)

static void
amqp_header_str(amqp_header_t *m, bytestream_t *bs)
{
    bytestream_nprintf(bs, 1024, "<basic.header ");
    FSTR(class_id, "%hd");
    FSTR(weight, "%hd");
    FSTR(body_size, "%lld");
    FSTR(flags, "%04hx");
    HFSTRB(CONTENT_TYPE, content_type);
    HFSTRB(CONTENT_ENCODING, content_encoding);
    HFSTRT(HEADERS, headers);
    HFSTR(DELIVERY_MODE, delivery_mode, "%hhd");
    HFSTR(PRIORITY, priority, "%hhd");
    HFSTRB(CORRELATION_ID, correlation_id);
    HFSTRB(REPLY_TO, reply_to);
    HFSTRB(EXPIRATION, expiration);
    HFSTRB(MESSAGE_ID, message_id);
    HFSTR(TIMESTAMP, timestamp, "%llx");
    HFSTRB(TYPE, type);
    HFSTRB(USER_ID, user_id);
    HFSTRB(APP_ID, app_id);
    HFSTRB(CLUSTER_ID, cluster_id);
    SADVANCEEOD(bs, -1);
    bytestream_cat(bs, 2, ">");
}


void
amqp_header_dump(amqp_header_t *header)
{
    bytestream_t bs;

    bytestream_init(&bs, 1024);
    amqp_header_str(header, &bs);
    TRACEC("%s", SDATA(&bs, 0));
    bytestream_fini(&bs);
}


void
amqp_header_destroy(amqp_header_t **header)
{
    if (*header != NULL) {
        BYTES_DECREF(&(*header)->content_type);
        BYTES_DECREF(&(*header)->content_encoding);
        dict_fini(&(*header)->headers);
        BYTES_DECREF(&(*header)->correlation_id);
        BYTES_DECREF(&(*header)->reply_to);
        BYTES_DECREF(&(*header)->expiration);
        BYTES_DECREF(&(*header)->message_id);
        BYTES_DECREF(&(*header)->type);
        BYTES_DECREF(&(*header)->user_id);
        BYTES_DECREF(&(*header)->app_id);
        BYTES_DECREF(&(*header)->cluster_id);
        free(*header);
    }
}



#define FHUNPACK(f, ty, n) if (m->flags & AMQP_HEADER_F##f)    \
{                                                              \
    FUNPACK(ty, n)                                             \
}                                                              \


#define FHPACK(f, ty, n) if (m->flags & AMQP_HEADER_F##f)      \
{                                                              \
    FPACK(ty, n);                                              \
}                                                              \


#define FHPACKA(f, ty, n) if (m->flags & AMQP_HEADER_F##f)     \
{                                                              \
    FPACKA(ty, n);                                             \
}                                                              \


int
amqp_header_dec(struct _amqp_conn *conn,
                   amqp_header_t **header)
{
    amqp_header_t *m;

    m = amqp_header_new();

    FUNPACK(short, class_id);
    FUNPACK(short, weight);
    FUNPACK(longlong, body_size);
    FUNPACK(short, flags);

    FHUNPACK(CONTENT_TYPE, shortstr, content_type);
    FHUNPACK(CONTENT_ENCODING, shortstr, content_encoding);
    FHUNPACK(HEADERS, table, headers);
    FHUNPACK(DELIVERY_MODE, octet, delivery_mode);
    FHUNPACK(PRIORITY, octet, priority);
    FHUNPACK(CORRELATION_ID, shortstr, correlation_id);
    FHUNPACK(REPLY_TO, shortstr, reply_to);
    FHUNPACK(EXPIRATION, shortstr, expiration);
    FHUNPACK(MESSAGE_ID, shortstr, message_id);
    FHUNPACK(TIMESTAMP, longlong, timestamp);
    FHUNPACK(TYPE, shortstr, type);
    FHUNPACK(USER_ID, shortstr, user_id);
    FHUNPACK(APP_ID, shortstr, app_id);
    FHUNPACK(CLUSTER_ID, shortstr, cluster_id);

    *header = m;
    return 0;
}


int
amqp_header_enc(amqp_header_t *m, struct _amqp_conn *conn)
{
    FPACK(short, class_id);
    FPACK(short, weight);
    FPACK(longlong, body_size);
    FPACK(short, flags);

    FHPACK(CONTENT_TYPE, shortstr, content_type);
    FHPACK(CONTENT_ENCODING, shortstr, content_encoding);
    FHPACKA(HEADERS, table, headers);
    FHPACK(DELIVERY_MODE, octet, delivery_mode);
    FHPACK(PRIORITY, octet, priority);
    FHPACK(CORRELATION_ID, shortstr, correlation_id);
    FHPACK(REPLY_TO, shortstr, reply_to);
    FHPACK(EXPIRATION, shortstr, expiration);
    FHPACK(MESSAGE_ID, shortstr, message_id);
    FHPACK(TIMESTAMP, longlong, timestamp);
    FHPACK(TYPE, shortstr, type);
    FHPACK(USER_ID, shortstr, user_id);
    FHPACK(APP_ID, shortstr, app_id);
    FHPACK(CLUSTER_ID, shortstr, cluster_id);

    return 0;
}

#define AMQP_HEADER_SET(n, f, ty)                      \
AMQP_HEADER_SET_DECL(n, ty)                            \
{                                                      \
    header->flags |= AMQP_HEADER_F##f;                 \
    header->n = v;                                     \
}                                                      \


AMQP_HEADER_SET(content_type, CONTENT_TYPE, bytes_t *)

AMQP_HEADER_SET(content_encoding, CONTENT_ENCODING, bytes_t *)

void
amqp_header_set_headers(amqp_header_t *header,
                        bytes_t *key,
                        amqp_value_t *value)
{
    header->flags |= AMQP_HEADER_FHEADERS;
    dict_set_item(&header->headers, key, value);
}

AMQP_HEADER_SET(delivery_mode, DELIVERY_MODE, uint8_t)

AMQP_HEADER_SET(priority, PRIORITY, uint8_t)

AMQP_HEADER_SET(correlation_id, CORRELATION_ID, bytes_t *)

AMQP_HEADER_SET(reply_to, REPLY_TO, bytes_t *)

AMQP_HEADER_SET(expiration, EXPIRATION, bytes_t *)

AMQP_HEADER_SET(message_id, MESSAGE_ID, bytes_t *)

AMQP_HEADER_SET(timestamp, TIMESTAMP, uint64_t)

AMQP_HEADER_SET(type, TYPE, bytes_t *)

AMQP_HEADER_SET(user_id, USER_ID, bytes_t *)

AMQP_HEADER_SET(app_id, APP_ID, bytes_t *)

AMQP_HEADER_SET(cluster_id, CLUSTER_ID, bytes_t *)


void
amqp_spec_init(void)
{
    size_t i;

    dict_init(&methods, 101,
              (dict_hashfn_t)method_info_hash,
              (dict_item_comparator_t)method_info_cmp,
              NULL);

    for (i = 0; i < countof(_methinfo); ++i) {
        amqp_method_info_t *mi;

        mi = &_methinfo[i];
        dict_set_item(&methods, (void *)(uintptr_t)mi->mid, mi);
    }
}


void
amqp_spec_fini(void)
{
    dict_fini(&methods);
}
