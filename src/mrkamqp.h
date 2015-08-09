#ifndef MRKAMQP_H_DEFINED
#define MRKAMQP_H_DEFINED

#include <mrkcommon/array.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/dict.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * wire
 */
#define AMQP_TBOOL 't'
#define AMQP_TINT8 'b'
#define AMQP_TUINT8 'B'
#define AMQP_TINT16 'u'
#define AMQP_TUINT16 'U'
#define AMQP_TINT32 'i'
#define AMQP_TUINT32 'I'
#define AMQP_TINT64 'l'
#define AMQP_TUINT64 'L'
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

typedef void (*amqp_encode)(struct _amqp_value *, bytestream_t *);
typedef ssize_t (*amqp_decode)(struct _amqp_value *, bytestream_t *, int);
typedef size_t (*amqp_len)(struct _amqp_value *);
typedef void (*amqp_kill)(struct _amqp_value *);

typedef struct _amqp_type {
    amqp_encode enc;
    amqp_decode dec;
    amqp_len len;
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
        bytes_t *str;
        array_t a;
        dict_t t;
    } value;
} amqp_value_t;


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
#define AMQP_EXCHANGE_BIND AMQP_METHID(AMQP_EXCHANGE, 30)
#define AMQP_EXCHANGE_BIND_OK AMQP_METHID(AMQP_EXCHANGE, 31)
#define AMQP_EXCHANGE_UNBIND AMQP_METHID(AMQP_EXCHANGE, 40)
#define AMQP_EXCHANGE_UNBIND_OK AMQP_METHID(AMQP_EXCHANGE, 41)

#define AMQP_QUEUE 50
#define AMQP_BASIC 60
#define AMQP_CONFIRM 85
#define AMQP_TX 90


typedef uint64_t amqp_meth_id_t;
struct _amqp_conn;


typedef struct _amqp_meth_params {
} amqp_meth_params_t;

#define MPARAMS(mname, __fields)                                       \
typedef struct _amqp_##mname {                                         \
    amqp_meth_params_t base;                                           \
    __fields                                                           \
} amqp_##mname##_t;                                                    \
int amqp_##mname##_enc(amqp_meth_params_t *, struct _amqp_conn *);     \
int amqp_##mname##_dec(amqp_meth_params_t *, struct _amqp_conn *);     \


MPARAMS(connection_start,
    uint8_t version_major;
    uint8_t version_minor;
    dict_t server_properties;
    bytes_t *mechanisms;
    bytes_t *locales;
)


MPARAMS(connection_start_ok,
    dict_t client_properties;
    bytes_t *mechanism;
    bytes_t *response;
    bytes_t *locale;
)


MPARAMS(connection_secure,
    bytes_t *challenge;
)


MPARAMS(connection_secure_ok,
    bytes_t *response;
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
    bytes_t *virtual_host;
    bytes_t *capabilities;
    /* 0 insist */
    uint8_t flags;
)


MPARAMS(connection_open_ok,
    bytes_t *known_hosts;
)


MPARAMS(connection_close,
    uint16_t reply_code;
    bytes_t *reply_text;
    uint16_t class_id;
    uint16_t method_id;
)


MPARAMS(connection_close_ok,
)


MPARAMS(channel_open,
    bytes_t *out_of_band;
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
    bytes_t *reply_text;
    uint16_t class_id;
    uint16_t method_id;
)


MPARAMS(channel_close_ok,
)


MPARAMS(exchange_declare,
    uint16_t ticket;
    bytes_t *exchange;
    bytes_t *type;
    /*
     * 0 passive
     * 1 durable
     * 2 auto_delete
     * 3 internal
     * 4 nowait
     */
    uint8_t flags;
    dict_t arguments;
)


MPARAMS(exchange_declare_ok,
)


MPARAMS(exchange_delete,
    uint16_t ticket;
    bytes_t *exchange;
    /*
     * 0 if_unused
     * 1 nowait
     */
    uint8_t flags;

)


MPARAMS(exchange_delete_ok,
)


MPARAMS(exchange_bind,
    uint16_t ticket;
    bytes_t *destination;
    bytes_t *source;
    bytes_t *routing_key;
    /*
     * 0 nowait
     */
    uint8_t flags;
    dict_t arguments;
)


MPARAMS(exchange_bind_ok,
)


MPARAMS(exchange_unbind,
    uint16_t ticket;
    bytes_t *destination;
    bytes_t *source;
    bytes_t *routing_key;
    /*
     * 0 nowait
     */
    uint8_t flags;
    dict_t arguments;
)


MPARAMS(exchange_unbind_ok,
)





/**/
int amqp_method_decode(amqp_meth_params_t *,
                       amqp_meth_id_t,
                       struct _amqp_conn *);


/*
 * client
 */
typedef struct _amqp_conn {
    char *host;
    int port;
    char *user;
    char *password;
    char *vhost;
    int heartbeat;
    int frame_max;

    int fd;
    bytestream_t ins;
    bytestream_t outs;
} amqp_conn_t;


typedef struct _amqp_frame {
    uint32_t sz;
    uint16_t chan;
    uint8_t type;
} amqp_frame_t;

typedef struct _amqp_channel {
    amqp_conn_t *conn;
    int id;
    int confirm_mode:1;
} amqp_channel_t;


typedef struct _amqp_consumer {
    amqp_channel_t *chan;
    bytes_t *ctag;
} amqp_consumer_t;


/*
 * wire
 */
#define UNPACK_ECONSUME (-2)

void pack_octet(bytestream_t *, uint8_t);
ssize_t unpack_octet(bytestream_t *, int, uint8_t *);
void pack_short(bytestream_t *, uint16_t);
ssize_t unpack_short(bytestream_t *, int, uint16_t *);
void pack_long(bytestream_t *, uint32_t);
ssize_t unpack_long(bytestream_t *, int, uint32_t *);
void pack_longlong(bytestream_t *, uint64_t);
ssize_t unpack_longlong(bytestream_t *, int, uint64_t *v);
void pack_float(bytestream_t *, float);
ssize_t unpack_float(bytestream_t *, int, float *);
void pack_double(bytestream_t *, double);
ssize_t unpack_double(bytestream_t *, int, double *);
void pack_shortstr(bytestream_t *, bytes_t *);
ssize_t unpack_shortstr(bytestream_t *, int, bytes_t **);
void pack_longstr(bytestream_t *, bytes_t *);
ssize_t unpack_longstr(bytestream_t *, int, bytes_t **);
void pack_table(bytestream_t *, dict_t *);
ssize_t unpack_table(bytestream_t *, int, dict_t *);

int amqp_decode_table(bytestream_t *, int, amqp_value_t **);
void amqp_value_destroy(amqp_value_t **);


/*
 * module
 */
void mrkamqp_init(void);
void mrkamqp_fini(void);


#ifdef __cplusplus
}
#endif
#endif /* MRKAMQP_H_DEFINED */
