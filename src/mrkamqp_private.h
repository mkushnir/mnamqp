#ifndef MRKAMQP_PRIVATE_H_DEFINED
#define MRKAMQP_PRIVATE_H_DEFINED

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

#ifdef HAVE_BYTESWAP_H
#include <byteswap.h>
#endif

#ifndef htobe16
#define htobe16(x) bswap_16 (x)
#endif

#ifndef be16toh
#define be16toh(x) bswap_16 (x)
#endif

#ifndef htobe32
#define htobe32(x) bswap_32 (x)
#endif

#ifndef be32toh
#define be32toh(x) bswap_32 (x)
#endif

#ifndef htobe64
#define htobe64(x) bswap_64 (x)
#endif

#ifndef be64toh
#define be64toh(x) bswap_64 (x)
#endif

#include <mrkcommon/array.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/hash.h>
#include <mrkcommon/stqueue.h>

#include <mrkthr.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MRKAMQP_SYNC

/*
 * wire
 */
#define AMQP_TBOOL 't'
#define AMQP_TINT8 'b'
#define AMQP_TUINT8 'B'
#define AMQP_TINT16 'U'
#define AMQP_TUINT16 'u'
#define AMQP_TINT32 'I'
#define AMQP_TUINT32 'i'
#define AMQP_TINT64 'L'
#define AMQP_TUINT64 'l'
#define AMQP_TFLOAT 'f'
#define AMQP_TDOUBLE 'd'
#define AMQP_TDECIMAL 'D'
#define AMQP_TSSTR 's'
#define AMQP_TLSTR 'S'
#define AMQP_TARRAY 'A'
#define AMQP_TTSTAMP 'T'
#define AMQP_TTABLE 'F'
#define AMQP_TVOID 'V'

struct _amqp_value;
struct _amqp_conn;
struct _amqp_channel;
struct _amqp_rpc;
struct _amqp_meth_params;
struct _amqp_header;

typedef void (*amqp_encode)(struct _amqp_value *, mnbytestream_t *);
typedef ssize_t (*amqp_decode)(struct _amqp_value *, mnbytestream_t *, int);
typedef void (*amqp_kill)(struct _amqp_value *);

typedef struct _amqp_type {
    amqp_encode enc;
    amqp_decode dec;
    amqp_kill kill;
    uint8_t tag;
} amqp_type_t;

typedef struct _amqp_decimal {
    uint8_t places;
    int32_t value;
} amqp_decimal_t;

typedef struct _amqp_value {
    amqp_type_t *ty;
    union {
        char b;
        int8_t i8;
        uint8_t u8;
        int16_t i16;
        uint16_t u16;
        int32_t i32;
        uint32_t u32;
        int64_t i64;
        uint64_t u64;
        float f;
        double d;
        amqp_decimal_t dc;
        mnbytes_t *str;
        mnarray_t a;
        mnhash_t t;
    } value;
} amqp_value_t;

/*
 * frame
 */
#define AMQP_FMETHOD 1
#define AMQP_FHEADER 2
#define AMQP_FBODY 3
#define AMQP_FHEARTBEAT 8

typedef struct _amqp_frame {
    STQUEUE_ENTRY(_amqp_frame, link);
    union {
        struct _amqp_meth_params *params;
        struct _amqp_header *header;
        char *body;
    } payload;
    uint32_t sz;
    uint16_t chan;
    uint8_t type;
} amqp_frame_t;

#define AMQP_FRAME_TYPE_STR(ty)                \
(                                              \
    ty == AMQP_FMETHOD ? "METHOD" :            \
    ty == AMQP_FHEADER ? "HEADER" :            \
    ty == AMQP_FBODY ? "BODY" :                \
    ty == AMQP_FHEARTBEAT ? "HEARTBEAT" :      \
    "<unknown>"                                \
)                                              \



/*
 * method
 */
#define AMQP_METHID(cls, id) ((cls) << 16 | (id))

#define AMQP_CONNECTION 10
#define AMQP_CONNECTION_START AMQP_METHID(AMQP_CONNECTION, 10)
#define AMQP_CONNECTION_START_OK AMQP_METHID(AMQP_CONNECTION, 11)
#define AMQP_CONNECTION_SECURE AMQP_METHID(AMQP_CONNECTION, 20)
#define AMQP_CONNECTION_SECURE_OK AMQP_METHID(AMQP_CONNECTION, 21)
#define AMQP_CONNECTION_TUNE AMQP_METHID(AMQP_CONNECTION, 30)
#define AMQP_CONNECTION_TUNE_OK AMQP_METHID(AMQP_CONNECTION, 31)
#define AMQP_CONNECTION_OPEN AMQP_METHID(AMQP_CONNECTION, 40)
#define AMQP_CONNECTION_OPEN_OK AMQP_METHID(AMQP_CONNECTION, 41)
#define AMQP_CONNECTION_CLOSE AMQP_METHID(AMQP_CONNECTION, 50)
#define AMQP_CONNECTION_CLOSE_OK AMQP_METHID(AMQP_CONNECTION, 51)

#define AMQP_CHANNEL 20
#define AMQP_CHANNEL_OPEN AMQP_METHID(AMQP_CHANNEL, 10)
#define AMQP_CHANNEL_OPEN_OK AMQP_METHID(AMQP_CHANNEL, 11)
#define AMQP_CHANNEL_FLOW AMQP_METHID(AMQP_CHANNEL, 20)
#define AMQP_CHANNEL_FLOW_OK AMQP_METHID(AMQP_CHANNEL, 21)
#define AMQP_CHANNEL_CLOSE AMQP_METHID(AMQP_CHANNEL, 40)
#define AMQP_CHANNEL_CLOSE_OK AMQP_METHID(AMQP_CHANNEL, 41)

#define AMQP_ACCESS 30

#define AMQP_EXCHANGE 40
#define AMQP_EXCHANGE_DECLARE AMQP_METHID(AMQP_EXCHANGE, 10)
#define AMQP_EXCHANGE_DECLARE_OK AMQP_METHID(AMQP_EXCHANGE, 11)
#define AMQP_EXCHANGE_DELETE AMQP_METHID(AMQP_EXCHANGE, 20)
#define AMQP_EXCHANGE_DELETE_OK AMQP_METHID(AMQP_EXCHANGE, 21)

#define AMQP_QUEUE 50
#define AMQP_QUEUE_DECLARE AMQP_METHID(AMQP_QUEUE, 10)
#define AMQP_QUEUE_DECLARE_OK AMQP_METHID(AMQP_QUEUE, 11)
#define AMQP_QUEUE_BIND AMQP_METHID(AMQP_QUEUE, 20)
#define AMQP_QUEUE_BIND_OK AMQP_METHID(AMQP_QUEUE, 21)
#define AMQP_QUEUE_PURGE AMQP_METHID(AMQP_QUEUE, 30)
#define AMQP_QUEUE_PURGE_OK AMQP_METHID(AMQP_QUEUE, 31)
#define AMQP_QUEUE_DELETE AMQP_METHID(AMQP_QUEUE, 40)
#define AMQP_QUEUE_DELETE_OK AMQP_METHID(AMQP_QUEUE, 41)
#define AMQP_QUEUE_UNBIND AMQP_METHID(AMQP_QUEUE, 50)
#define AMQP_QUEUE_UNBIND_OK AMQP_METHID(AMQP_QUEUE, 51)

#define AMQP_BASIC 60
#define AMQP_BASIC_QOS AMQP_METHID(AMQP_BASIC, 10)
#define AMQP_BASIC_QOS_OK AMQP_METHID(AMQP_BASIC, 11)
#define AMQP_BASIC_CONSUME AMQP_METHID(AMQP_BASIC, 20)
#define AMQP_BASIC_CONSUME_OK AMQP_METHID(AMQP_BASIC, 21)
#define AMQP_BASIC_CANCEL AMQP_METHID(AMQP_BASIC, 30)
#define AMQP_BASIC_CANCEL_OK AMQP_METHID(AMQP_BASIC, 31)
#define AMQP_BASIC_PUBLISH AMQP_METHID(AMQP_BASIC, 40)
#define AMQP_BASIC_RETURN AMQP_METHID(AMQP_BASIC, 50)
#define AMQP_BASIC_DELIVER AMQP_METHID(AMQP_BASIC, 60)
#define AMQP_BASIC_GET AMQP_METHID(AMQP_BASIC, 70)
#define AMQP_BASIC_GET_OK AMQP_METHID(AMQP_BASIC, 71)
#define AMQP_BASIC_GET_EMPTY AMQP_METHID(AMQP_BASIC, 72)
#define AMQP_BASIC_ACK AMQP_METHID(AMQP_BASIC, 80)
#define AMQP_BASIC_REJECT AMQP_METHID(AMQP_BASIC, 90)
#define AMQP_BASIC_RECOVER_ASYNC AMQP_METHID(AMQP_BASIC, 100)
#define AMQP_BASIC_RECOVER AMQP_METHID(AMQP_BASIC, 110)
#define AMQP_BASIC_RECOVER_OK AMQP_METHID(AMQP_BASIC, 111)
#define AMQP_BASIC_NACK AMQP_METHID(AMQP_BASIC, 120)

#define AMQP_CONFIRM 85
#define AMQP_CONFIRM_SELECT AMQP_METHID(AMQP_CONFIRM, 10)
#define AMQP_CONFIRM_SELECT_OK AMQP_METHID(AMQP_CONFIRM, 11)

#define AMQP_TX 90


typedef struct _amqp_meth_params_t *(*amqp_method_new_t)(void);

typedef void (*amqp_method_str_t)(struct _amqp_meth_params *,
                                  mnbytestream_t *);

typedef int (*amqp_method_enc_t) (struct _amqp_meth_params *,
                                  struct _amqp_conn *);

typedef int (*amqp_method_dec_t) (struct _amqp_conn *,
                                  struct _amqp_meth_params **);

typedef void (*amqp_method_fini_t) (struct _amqp_meth_params *);

typedef uint64_t amqp_meth_id_t;

typedef struct _amqp_method_info {
    char *name;
    amqp_meth_id_t mid;
    amqp_method_new_t new_;
    amqp_method_str_t str;
    amqp_method_enc_t enc;
    amqp_method_dec_t dec;
    amqp_method_fini_t fini;
} amqp_method_info_t;


typedef struct _amqp_meth_params {
    amqp_method_info_t *mi;
} amqp_meth_params_t;

#define MPARAMS(mname, __fields)                                       \
typedef struct _amqp_##mname {                                         \
    amqp_meth_params_t base;                                           \
    __fields                                                           \
} amqp_##mname##_t;                                                    \
int amqp_##mname##_enc(amqp_meth_params_t *, struct _amqp_conn *);     \
int amqp_##mname##_dec(struct _amqp_conn *, amqp_meth_params_t **);    \
void amqp_##mname##_fini(amqp_meth_params_t *);                        \


/*
 * connection.*
 */
MPARAMS(connection_start,
    uint8_t version_major;
    uint8_t version_minor;
    mnhash_t server_properties;
    mnbytes_t *mechanisms;
    mnbytes_t *locales;
)


MPARAMS(connection_start_ok,
    mnhash_t client_properties;
    mnbytes_t *mechanism;
    mnbytes_t *response;
    mnbytes_t *locale;
)


MPARAMS(connection_secure,
    mnbytes_t *challenge;
)


MPARAMS(connection_secure_ok,
    mnbytes_t *response;
)


MPARAMS(connection_tune,
    uint16_t channel_max;
    uint32_t frame_max;
    uint16_t heartbeat;
)


MPARAMS(connection_tune_ok,
    uint16_t channel_max;
    uint32_t frame_max;
    uint16_t heartbeat;
)


MPARAMS(connection_open,
    mnbytes_t *virtual_host;
    mnbytes_t *capabilities;
    /* 0 insist */
    uint8_t flags;
)


MPARAMS(connection_open_ok,
    mnbytes_t *known_hosts;
)


MPARAMS(connection_close,
    uint16_t reply_code;
    mnbytes_t *reply_text;
    uint16_t class_id;
    uint16_t method_id;
)


MPARAMS(connection_close_ok,
)


/*
 * channel.*
 */
MPARAMS(channel_open,
    mnbytes_t *out_of_band;
)


MPARAMS(channel_open_ok,
    uint32_t channel_id;
)


MPARAMS(channel_flow,
    /* 0 active */
    uint8_t flags;
)


MPARAMS(channel_flow_ok,
    /* 0 active */
    uint8_t flags;
)



MPARAMS(channel_close,
    uint16_t reply_code;
    mnbytes_t *reply_text;
    uint16_t class_id;
    uint16_t method_id;
)


MPARAMS(channel_close_ok,
)



/*
 * confirm.*
 */
MPARAMS(confirm_select,
    /*
     * 0 nowait
     */
    uint8_t flags;
)

MPARAMS(confirm_select_ok,
)




/*
 * exchange.*
 */
MPARAMS(exchange_declare,
    uint16_t ticket;
    mnbytes_t *exchange;
    mnbytes_t *type;
    /*
     * 0 passive
     * 1 durable
     * 2 auto_delete
     * 3 internal
     * 4 nowait
     */
    uint8_t flags;
    mnhash_t arguments;
)


MPARAMS(exchange_declare_ok,
)


MPARAMS(exchange_delete,
    uint16_t ticket;
    mnbytes_t *exchange;
    /*
     * 0 if_unused
     * 1 nowait
     */
    uint8_t flags;

)


MPARAMS(exchange_delete_ok,

)


/*
 * queue.*
 */
MPARAMS(queue_declare,
    uint16_t ticket;
    mnbytes_t *queue;
    /*
     * 0 passive
     * 1 durable
     * 2 exclusive
     * 3 auto_delete
     * 4 nowait
     */
    uint8_t flags;
    mnhash_t arguments;
)


MPARAMS(queue_declare_ok,
    mnbytes_t *queue;
    uint32_t message_count;
    uint32_t consumer_count;
)


MPARAMS(queue_bind,
    uint16_t ticket;
    mnbytes_t *queue;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
    /*
     * 0 nowait
     */
    uint8_t flags;
    mnhash_t arguments;
)


MPARAMS(queue_bind_ok,
)


MPARAMS(queue_purge,
    uint16_t ticket;
    mnbytes_t *queue;
    /*
     * 0 nowait
     */
    uint8_t flags;
)


MPARAMS(queue_purge_ok,
    uint32_t message_count;
)


MPARAMS(queue_delete,
    uint16_t ticket;
    mnbytes_t *queue;
    /*
     * 0 if_unused
     * 1 if_empty
     * 2 nowait
     */
    uint8_t flags;
)


MPARAMS(queue_delete_ok,
    uint32_t message_count;
)


MPARAMS(queue_unbind,
    uint16_t ticket;
    mnbytes_t *queue;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
    mnhash_t arguments;
)


MPARAMS(queue_unbind_ok,
)


/*
 * basic.*
 */
MPARAMS(basic_qos,
    uint32_t prefetch_size;
    uint16_t prefetch_count;
    /*
     * 0 global
     */
    uint8_t flags;
)


MPARAMS(basic_qos_ok,
)


MPARAMS(basic_consume,
    uint16_t ticket;
    mnbytes_t *queue;
    mnbytes_t *consumer_tag;
    /*
     * 0 no_local
     * 1 no_ack
     * 2 exclusive
     * 3 nowait
     */
    uint8_t flags;
    mnhash_t arguments;
)


MPARAMS(basic_consume_ok,
    mnbytes_t *consumer_tag;
)


MPARAMS(basic_cancel,
    mnbytes_t *consumer_tag;
    /*
     * 0 nowait
     */
    uint8_t flags;
)


MPARAMS(basic_cancel_ok,
    mnbytes_t *consumer_tag;
)


MPARAMS(basic_publish,
    uint16_t ticket;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
    /*
     * 0 mandatory
     * 1 immediate
     */
    uint8_t flags;
)


MPARAMS(basic_return,
    uint16_t reply_code;
    mnbytes_t *reply_text;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
)


MPARAMS(basic_deliver,
    mnbytes_t *consumer_tag;
    uint64_t delivery_tag;
    /*
     * 0 redelivered
     */
    uint8_t flags;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
)


MPARAMS(basic_get,
    uint16_t ticket;
    mnbytes_t *queue;
    /*
     * 0 no_ack
     */
    uint8_t flags;
)


MPARAMS(basic_get_ok,
    uint64_t delivery_tag;
    /*
     * 0 redelivered
     */
    uint8_t flags;
    mnbytes_t *exchange;
    mnbytes_t *routing_key;
    uint32_t message_count;
)


MPARAMS(basic_get_empty,
    mnbytes_t *cluster_id;
)


MPARAMS(basic_ack,
    uint64_t delivery_tag;
    /*
     * 0 multiple
     */
    uint8_t flags;
)


MPARAMS(basic_reject,
    uint64_t delivery_tag;
    /*
     * 0 requeue
     */
    uint8_t flags;
)


MPARAMS(basic_recover_async,
    /*
     * 0 requeue
     */
    uint8_t flags;
)


MPARAMS(basic_recover,
    /*
     * 0 requeue
     */
    uint8_t flags;
)


MPARAMS(basic_recover_ok,
)


MPARAMS(basic_nack,
    uint64_t delivery_tag;
    /*
     * 0 multiple
     * 1 requeue
     */
    uint8_t flags;
)

/*
 * wire API
 */
#define UNPACK_ECONSUME (-2)
#define UNPACK_ETAG (-3)

void pack_octet(mnbytestream_t *, uint8_t);
ssize_t unpack_octet(mnbytestream_t *, int, uint8_t *);
void pack_short(mnbytestream_t *, uint16_t);
ssize_t unpack_short(mnbytestream_t *, int, uint16_t *);
void pack_long(mnbytestream_t *, uint32_t);
ssize_t unpack_long(mnbytestream_t *, int, uint32_t *);
void pack_longlong(mnbytestream_t *, uint64_t);
ssize_t unpack_longlong(mnbytestream_t *, int, uint64_t *v);
void pack_float(mnbytestream_t *, float);
ssize_t unpack_float(mnbytestream_t *, int, float *);
void pack_double(mnbytestream_t *, double);
ssize_t unpack_double(mnbytestream_t *, int, double *);
void pack_shortstr(mnbytestream_t *, mnbytes_t *);
ssize_t unpack_shortstr(mnbytestream_t *, int, mnbytes_t **);
void pack_longstr(mnbytestream_t *, mnbytes_t *);
ssize_t unpack_longstr(mnbytestream_t *, int, mnbytes_t **);
void pack_table(mnbytestream_t *, mnhash_t *);
ssize_t unpack_table(mnbytestream_t *, int, mnhash_t *);
void init_table(mnhash_t *);

int amqp_decode_table(mnbytestream_t *, int, amqp_value_t **);
amqp_value_t *amqp_value_new(uint8_t);
void amqp_value_destroy(amqp_value_t **);


#define TABLE_ADD_REF(n, ty_)                          \
int table_add_##n(mnhash_t *v, const char *key, ty_ val) \


TABLE_ADD_REF(boolean, char);
TABLE_ADD_REF(i8, int8_t);
TABLE_ADD_REF(u8, uint8_t);
TABLE_ADD_REF(i16, int16_t);
TABLE_ADD_REF(u16, uint16_t);
TABLE_ADD_REF(i32, int32_t);
TABLE_ADD_REF(u32, uint32_t);
TABLE_ADD_REF(i64, int64_t);
TABLE_ADD_REF(u64, uint64_t);
TABLE_ADD_REF(float, float);
TABLE_ADD_REF(double, double);
// RabbitMQ doesn't like short str?
//TABLE_ADD_REF(sstr, mnbytes_t *);
TABLE_ADD_REF(lstr, mnbytes_t *);
int table_add_value(mnhash_t *, const char *, amqp_value_t *);
void table_str(mnhash_t *, mnbytestream_t *);


amqp_value_t *table_get_value(mnhash_t *, mnbytes_t *);

/*
 * frame API
 */
amqp_frame_t *amqp_frame_new(uint16_t, uint8_t);
void amqp_frame_destroy_method(amqp_frame_t **);
void amqp_frame_destroy_header(amqp_frame_t **);
void amqp_frame_destroy_body(amqp_frame_t **);
void amqp_frame_destroy(amqp_frame_t **);
void amqp_frame_dump(amqp_frame_t *);


/*
 * spec API
 */
void amqp_spec_init(void);
void amqp_spec_fini(void);
amqp_method_info_t *amqp_method_info_get(amqp_meth_id_t);



#define NEWREF(mname) amqp_##mname##_new
#define NEWDECL(mname) amqp_##mname##_t *NEWREF(mname)(void)

NEWDECL(connection_start_ok);
NEWDECL(connection_tune_ok);
NEWDECL(connection_open);
NEWDECL(connection_close);
NEWDECL(connection_close_ok);
NEWDECL(channel_open);
NEWDECL(channel_flow);
NEWDECL(channel_close);
NEWDECL(confirm_select);
NEWDECL(exchange_declare);
NEWDECL(exchange_delete);
NEWDECL(queue_declare);
NEWDECL(queue_bind);
NEWDECL(queue_unbind);
NEWDECL(queue_purge);
NEWDECL(queue_delete);
NEWDECL(basic_qos);
NEWDECL(basic_consume);
NEWDECL(basic_cancel);
NEWDECL(basic_cancel_ok);
NEWDECL(basic_publish);
NEWDECL(basic_deliver);
NEWDECL(basic_ack);
NEWDECL(basic_nack);


/*
 * method API
 */
int amqp_meth_params_decode(struct _amqp_conn *,
                            amqp_meth_id_t,
                            amqp_meth_params_t **);
void amqp_meth_params_dump(amqp_meth_params_t *);
void amqp_meth_params_destroy(amqp_meth_params_t **);


/*
 * extended channel API
 */
typedef void (*amqp_frame_completion_cb_t)(struct _amqp_channel *,
                                           amqp_frame_t *,
                                           void *);
MRKAMQP_SYNC int amqp_channel_declare_exchange_ex(struct _amqp_channel *,
                                                  const char *,
                                                  const char *,
                                                  uint8_t,
                                                  amqp_frame_completion_cb_t,
                                                  amqp_frame_completion_cb_t,
                                                  amqp_frame_completion_cb_t,
                                                  void *);
MRKAMQP_SYNC int amqp_channel_declare_queue_ex(struct _amqp_channel *,
                                               const char *,
                                               uint8_t,
                                               amqp_frame_completion_cb_t,
                                               amqp_frame_completion_cb_t,
                                               amqp_frame_completion_cb_t,
                                               void *);
MRKAMQP_SYNC int amqp_channel_bind_queue_ex(struct _amqp_channel *,
                                            const char *,
                                            const char *,
                                            const char *,
                                            uint8_t,
                                            amqp_frame_completion_cb_t,
                                            amqp_frame_completion_cb_t,
                                            amqp_frame_completion_cb_t,
                                            void *udata);
MRKAMQP_SYNC int amqp_channel_unbind_queue_ex(struct _amqp_channel *,
                                              const char *,
                                              const char *,
                                              const char *,
                                              amqp_frame_completion_cb_t,
                                              amqp_frame_completion_cb_t,
                                              amqp_frame_completion_cb_t,
                                              void *);

#ifdef __cplusplus
}
#endif

#include <mrkamqp.h>

#endif /* MRKAMQP_PRIVATE_H_DEFINED */
