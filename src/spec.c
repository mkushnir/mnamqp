#include <mrkcommon/bytestream.h>
#include <mrkcommon/dict.h>
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkamqp.h>

#include "diag.h"

dict_t methods;


#define FPACK(ty, name) pack_##ty(&conn->outs, m->name)
#define FPACKA(ty, name) pack_##ty(&conn->outs, &m->name)
#define ENC(mname, __fields)                                           \
int                                                                    \
amqp_##mname##_enc(amqp_meth_params_t *self, amqp_conn_t *conn)        \
{                                                                      \
    amqp_##mname##_t *m;                                               \
    m = (amqp_##mname##_t *)self;                                      \
    __fields;                                                          \
    return 0;                                                          \
}                                                                      \


#define ENCE(mname)                                    \
int                                                    \
amqp_##mname##_enc(UNUSED amqp_meth_params_t *self,    \
                       UNUSED amqp_conn_t *conn)       \
{                                                      \
    return 0;                                          \
}                                                      \


#define FUNPACK(ty, name)                              \
if (unpack_##ty(&conn->ins, conn->fd, &m->name) < 0) { \
    TRRET(UNPACK + 100);                               \
}                                                      \


#define DEC(mname, __fields)                                   \
int                                                            \
amqp_##mname##_dec(amqp_meth_params_t *self, amqp_conn_t *conn)\
{                                                              \
    amqp_##mname##_t *m;                                       \
    m = (amqp_##mname##_t *)self;                              \
    __fields;                                                  \
    return 0;                                                  \
}                                                              \


#define DECE(mname)                                    \
int                                                    \
amqp_##mname##_dec(UNUSED amqp_meth_params_t *self,    \
                   UNUSED amqp_conn_t *conn)           \
{                                                      \
    return 0;                                          \
}                                                      \



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


ENC(connection_secure,
    FPACK(longstr, challenge);
)

DEC(connection_secure,
    FUNPACK(longstr, challenge);
)


ENC(connection_secure_ok,
    FPACK(longstr, response);
)

DEC(connection_secure_ok,
    FUNPACK(longstr, response);
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


ENC(connection_open_ok,
    FPACK(shortstr, known_hosts);
)

DEC(connection_open_ok,
    FUNPACK(shortstr, known_hosts);
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


ENCE(connection_close_ok)

DECE(connection_close_ok)


ENC(channel_open,
    FPACK(shortstr, out_of_band);
)

DEC(channel_open,
    FUNPACK(shortstr, out_of_band);
)


ENC(channel_open_ok,
    FPACK(long, channel_id);
)

DEC(channel_open_ok,
    FUNPACK(long, channel_id);
)


ENC(channel_flow,
    FPACK(octet, flags);
)

DEC(channel_flow,
    FUNPACK(octet, flags);
)


ENC(channel_flow_ok,
    FPACK(octet, flags);
)

DEC(channel_flow_ok,
    FUNPACK(octet, flags);
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


ENCE(channel_close_ok)

DECE(channel_close_ok)


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


ENCE(exchange_declare_ok)

DECE(exchange_declare_ok)


ENC(exchange_delete,
    FPACK(short, ticket);
    FPACK(octet, flags);
)

DEC(exchange_delete,
    FUNPACK(short, ticket);
    FUNPACK(octet, flags);
)


ENCE(exchange_delete_ok)

DECE(exchange_delete_ok)


ENC(exchange_bind,
    FPACK(short, ticket);
    FPACK(shortstr, destination);
    FPACK(shortstr, source);
    FPACK(shortstr, routing_key);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)

DEC(exchange_bind,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, destination);
    FUNPACK(shortstr, source);
    FUNPACK(shortstr, routing_key);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)


ENCE(exchange_bind_ok)

DECE(exchange_bind_ok)


ENC(exchange_unbind,
    FPACK(short, ticket);
    FPACK(shortstr, destination);
    FPACK(shortstr, source);
    FPACK(shortstr, routing_key);
    FPACK(octet, flags);
    FPACKA(table, arguments);
)

DEC(exchange_unbind,
    FUNPACK(short, ticket);
    FUNPACK(shortstr, destination);
    FUNPACK(shortstr, source);
    FUNPACK(shortstr, routing_key);
    FUNPACK(octet, flags);
    FUNPACK(table, arguments);
)


ENCE(exchange_unbind_ok)

DECE(exchange_unbind_ok)



/*
* registry
*/
typedef int (*amqp_method_enc_t) (amqp_meth_params_t *,
                              amqp_conn_t *);
typedef int (*amqp_method_dec_t) (amqp_meth_params_t *,
                                  amqp_conn_t *);

typedef struct _amqp_method_info {
    amqp_meth_id_t mid;
    amqp_method_enc_t enc;
    amqp_method_dec_t dec;
} amqp_method_info_t;

#define MI(id, mname) {id, amqp_##mname##_enc, amqp_##mname##_dec}

UNUSED static amqp_method_info_t _methinfo[] = {
    MI(AMQP_CONNECTION_START, connection_start),
    MI(AMQP_CONNECTION_START_OK, connection_start_ok),
    MI(AMQP_CONNECTION_SECURE, connection_secure),
    MI(AMQP_CONNECTION_SECURE_OK, connection_secure_ok),
    MI(AMQP_CONNECTION_TUNE, connection_tune),
    MI(AMQP_CONNECTION_TUNE_OK, connection_tune_ok),
    MI(AMQP_CONNECTION_OPEN, connection_open),
    MI(AMQP_CONNECTION_OPEN_OK, connection_open_ok),
    MI(AMQP_CONNECTION_CLOSE, connection_close),
    MI(AMQP_CONNECTION_CLOSE_OK, connection_close_ok),

    MI(AMQP_CHANNEL_OPEN, channel_open),
    MI(AMQP_CHANNEL_OPEN_OK, channel_open_ok),
    MI(AMQP_CHANNEL_FLOW, channel_flow),
    MI(AMQP_CHANNEL_FLOW_OK, channel_flow_ok),
    MI(AMQP_CHANNEL_CLOSE, channel_close),
    MI(AMQP_CHANNEL_CLOSE_OK, channel_close_ok),

    MI(AMQP_EXCHANGE_DECLARE, exchange_declare),
    MI(AMQP_EXCHANGE_DECLARE_OK, exchange_declare_ok),
    MI(AMQP_EXCHANGE_DELETE, exchange_delete),
    MI(AMQP_EXCHANGE_DELETE_OK, exchange_delete_ok),
    MI(AMQP_EXCHANGE_BIND, exchange_bind),
    MI(AMQP_EXCHANGE_BIND_OK, exchange_bind_ok),
    MI(AMQP_EXCHANGE_UNBIND, exchange_unbind),
    MI(AMQP_EXCHANGE_UNBIND_OK, exchange_unbind_ok),
};


int
amqp_method_decode(amqp_meth_params_t *params,
                   amqp_meth_id_t mid,
                   amqp_conn_t *conn)
{
    dict_item_t *dit;
    amqp_method_info_t *mi;

    if ((dit = dict_get_item(&methods, (void *)(uintptr_t)mid)) == NULL) {
        TRRET(AMQP_METHOD_DECODE + 1);
    }
    mi = dit->value;
    assert(mi->mid == mid);
    return mi->dec(params, conn);
}
