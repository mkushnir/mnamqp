#ifndef MRKAMQP_H_DEFINED
#define MRKAMQP_H_DEFINED

#include <mrkcommon/array.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/hash.h>
#include <mrkcommon/stqueue.h>

#include <mrkthr.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MRKAMQP_SYNC
#define MRKAMQP_SYNC
#endif

struct _amqp_channel;
struct _amqp_consumer;

typedef struct _amqp_conn {
    char *host;
    int port;
    char *user;
    char *password;
    char *vhost;
    uint16_t channel_max;
    uint32_t frame_max;
    uint32_t payload_max;
    uint16_t heartbeat;
    int capabilities;

    int fd;
    bytestream_t ins;
    bytestream_t outs;
    mrkthr_ctx_t *recv_thread;
    mrkthr_ctx_t *send_thread;
    mrkthr_ctx_t *heartbeat_thread;
    uint64_t last_sock_op;
    /* outgoing frames */
    STQUEUE(_amqp_frame, oframes);
    mrkthr_signal_t oframe_sig;
    mrkthr_signal_t ping_sig;

    array_t channels;
    struct _amqp_channel *chan0;
    uint16_t error_code;
    bytes_t *error_msg;
    int closed:1;
} amqp_conn_t;


typedef struct _amqp_pending_pub {
    DTQUEUE_ENTRY(_amqp_pending_pub, link);
    mrkthr_signal_t sig;
    uint64_t publish_tag;
} amqp_pending_pub_t;


typedef struct _amqp_channel {
    amqp_conn_t *conn;
    /* incoming frames */
    STQUEUE(_amqp_frame, iframes);
    mrkthr_signal_t expect_sig;
    mrkthr_sema_t sync_sema;
    hash_t consumers;
    /* strong ref */
    struct _amqp_consumer *default_consumer;
    /* weak ref */
    struct _amqp_consumer *content_consumer;
    uint64_t publish_tag;
    DTQUEUE(_amqp_pending_pub, pending_pub);
    int id;
    int confirm_mode:1;
    int closed:1;
} amqp_channel_t;


typedef struct _amqp_pending_content {
    STQUEUE_ENTRY(_amqp_pending_content, link);
    amqp_frame_t *method;
    amqp_frame_t *header;
    STQUEUE(_amqp_frame, body);
} amqp_pending_content_t;

typedef int (*amqp_consumer_content_cb_t)(amqp_frame_t *,
                                          amqp_frame_t *,
                                          char *,
                                          void *);

typedef struct _amqp_consumer {
    amqp_channel_t *chan;
    bytes_t *consumer_tag;
    mrkthr_signal_t content_sig;
    STQUEUE(_amqp_pending_content, pending_content);
    mrkthr_ctx_t *content_thread;
    amqp_consumer_content_cb_t content_cb;
    amqp_consumer_content_cb_t cancel_cb;
    void *content_udata;
    uint8_t flags;
    int closed:1;
} amqp_consumer_t;


/*
 * header
 */
typedef struct _amqp_header {
    uint16_t class_id;
    uint16_t weight;
    uint64_t body_size;
#define AMQP_HEADER_FCONTENT_TYPE       (1 << 15)
#define AMQP_HEADER_FCONTENT_ENCODING   (1 << 14)
#define AMQP_HEADER_FHEADERS            (1 << 13)
#define AMQP_HEADER_FDELIVERY_MODE      (1 << 12)
#define AMQP_HEADER_FPRIORITY           (1 << 11)
#define AMQP_HEADER_FCORRELATION_ID     (1 << 10)
#define AMQP_HEADER_FREPLY_TO           (1 << 9)
#define AMQP_HEADER_FEXPIRATION         (1 << 8)
#define AMQP_HEADER_FMESSAGE_ID         (1 << 7)
#define AMQP_HEADER_FTIMESTAMP          (1 << 6)
#define AMQP_HEADER_FTYPE               (1 << 5)
#define AMQP_HEADER_FUSER_ID            (1 << 4)
#define AMQP_HEADER_FAPP_ID             (1 << 3)
#define AMQP_HEADER_FCLUSTER_ID         (1 << 2)
    uint16_t flags;
    bytes_t *content_type;
    bytes_t *content_encoding;
    hash_t headers;
    uint8_t delivery_mode;
    uint8_t priority;
    bytes_t *correlation_id;
    bytes_t *reply_to;
    bytes_t *expiration;
    bytes_t *message_id;
    uint64_t timestamp;
    bytes_t *type;
    bytes_t *user_id;
    bytes_t *app_id;
    bytes_t *cluster_id;

    uint64_t _received_size;
} amqp_header_t;


/*
 * rpc
 */
typedef void (*amqp_rpc_server_handler_t)(const amqp_header_t *,
                                          const char *,
                                          amqp_header_t **,
                                          char **,
                                          void *);

typedef void (*amqp_rpc_request_header_cb_t)(amqp_header_t *,
                                             void *);
typedef struct _amqp_rpc {
    char *exchange;
    char *routing_key;
    bytes_t *reply_to;
    /* weakref */
    amqp_channel_t *chan;
    amqp_consumer_t *cons;
    /* key weakref, value weakref */
    hash_t calls;
    uint64_t next_id;
    amqp_consumer_content_cb_t cccb;
    amqp_consumer_content_cb_t clcb;
    amqp_rpc_server_handler_t server_handler;
    void *server_udata;
} amqp_rpc_t;


/*
 * conn
 */
#define AMQP_CAP_PUBLISHER_CONFIRMS (0x01)
#define AMQP_CAP_CONSUMER_CANCEL_NOTIFY (0x02)
amqp_conn_t *amqp_conn_new(const char *,
                           int,
                           const char *,
                           const char *,
                           const char *,
                           short,
                           int,
                           short,
                           int);
size_t amqp_conn_oframes_length(amqp_conn_t *);
void amqp_conn_destroy(amqp_conn_t **);
int amqp_conn_open(amqp_conn_t *);
MRKAMQP_SYNC int amqp_conn_run(amqp_conn_t *);
int amqp_conn_ping(amqp_conn_t *);
#define AMQP_CONN_CLOSE_FFAST (0x01)
MRKAMQP_SYNC int amqp_conn_close(amqp_conn_t *, int);
void amqp_conn_post_close(amqp_conn_t *);


/*
 * channel
 */
MRKAMQP_SYNC amqp_channel_t *amqp_create_channel(amqp_conn_t *);
size_t amqp_channel_iframes_length(amqp_channel_t *);
#define CHANNEL_CONFIRM_FNOWAIT         0x01
MRKAMQP_SYNC int amqp_channel_confirm(amqp_channel_t *, uint8_t);
MRKAMQP_SYNC int amqp_close_channel(amqp_channel_t *);
void amqp_close_channel_fast(amqp_channel_t *);

#define DECLARE_EXCHANGE_FPASSIVE       0x01
#define DECLARE_EXCHANGE_FDURABLE       0x02
#define DECLARE_EXCHANGE_FAUTODELETE    0x04
#define DECLARE_EXCHANGE_FINTERNAL      0x08
#define DECLARE_EXCHANGE_FNOWAIT        0x10
MRKAMQP_SYNC int amqp_channel_declare_exchange(amqp_channel_t *,
                                               const char *,
                                               const char *,
                                               uint8_t);

#define DELETE_EXCHANGE_FIFUNUSED       0x01
#define DELETE_EXCHANGE_FNOWAIT         0x02
MRKAMQP_SYNC int amqp_channel_delete_exchange(amqp_channel_t *,
                                              const char *,
                                              uint8_t);

#define DECLARE_QUEUE_FPASSIVE          0x01
#define DECLARE_QUEUE_FDURABLE          0x02
#define DECLARE_QUEUE_FEXCLUSIVE        0x04
#define DECLARE_QUEUE_FAUTODELETE       0x08
#define DECLARE_QUEUE_FNOWAIT           0x10
MRKAMQP_SYNC int amqp_channel_declare_queue(amqp_channel_t *,
                                            const char *,
                                            uint8_t);

#define BIND_QUEUE_FNOWAIT              0x01
MRKAMQP_SYNC int amqp_channel_bind_queue(amqp_channel_t *,
                                         const char *,
                                         const char *,
                                         const char *,
                                         uint8_t);

MRKAMQP_SYNC int amqp_channel_unbind_queue(amqp_channel_t *,
                                           const char *,
                                           const char *,
                                           const char *);

#define PURGE_QUEUE_FNOWAIT             0x01
MRKAMQP_SYNC int amqp_channel_purge_queue(amqp_channel_t *,
                                          const char *,
                                          uint8_t);

#define DELETE_QUEUE_FIFUNUSED          0x01
#define DELETE_QUEUE_FIFEMPTY           0x02
#define DELETE_QUEUE_FNOWAIT            0x04
MRKAMQP_SYNC int amqp_channel_delete_queue(amqp_channel_t *,
                              const char *,
                              uint8_t);

#define QOS_FGLOBAL                     0x01
MRKAMQP_SYNC int amqp_channel_qos(amqp_channel_t *,
                     uint32_t,
                     uint16_t,
                     uint8_t);

#define FLOW_ACTIVE                     0x01
MRKAMQP_SYNC int amqp_channel_flow(amqp_channel_t *, uint8_t);

#if 0 // see amqp_channel_create_consumer()
#define CONSUME_FNOLOCAL                0x01
#define CONSUME_FNOACK                  0x02
#define CONSUME_FEXCLUSIVE              0x04
#define CONSUME_FNOWAIT                 0x08
int amqp_channel_consume(amqp_channel_t *,
                         const char *,
                         const char *,
                         uint8_t);
#endif

#define CANCEL_FNOWAIT                  0x01
int amqp_channel_cancel(amqp_channel_t *,
                        const char *,
                        uint8_t);

#define PUBLISH_MANDATORY               0x01
#define PUBLISH_IMMEDIATE               0x02
typedef void (*amqp_header_completion_cb)(amqp_channel_t *,
                                          amqp_header_t *,
                                          void *);

MRKAMQP_SYNC int amqp_channel_publish(amqp_channel_t *,
                         const char *,
                         const char *,
                         uint8_t,
                         amqp_header_completion_cb,
                         void *,
                         const char *,
                         ssize_t);

MRKAMQP_SYNC int amqp_channel_publish_ex(amqp_channel_t *,
                            const char *,
                            const char *,
                            uint8_t,
                            amqp_header_t *,
                            const char *);

#define ACK_MULTIPLE                    0x01

void amqp_channel_drain_methods(amqp_channel_t *);

/*
 * consumer
 */
#define CONSUME_FNOLOCAL                0x01
#define CONSUME_FNOACK                  0x02
#define CONSUME_FEXCLUSIVE              0x04
#define CONSUME_FNOWAIT                 0x08
MRKAMQP_SYNC amqp_consumer_t *amqp_channel_create_consumer(amqp_channel_t *,
                                                           const char *,
                                                           const char *,
                                                           uint8_t);

amqp_consumer_t *amqp_channel_set_default_consumer(amqp_channel_t *);

int amqp_consumer_handle_content_spawn(amqp_consumer_t *,
                                       amqp_consumer_content_cb_t,
                                       amqp_consumer_content_cb_t,
                                       void *);

int amqp_consumer_handle_content(amqp_consumer_t *,
                                 amqp_consumer_content_cb_t,
                                 amqp_consumer_content_cb_t,
                                 void *);

MRKAMQP_SYNC int amqp_close_consumer(amqp_consumer_t *);
void amqp_close_consumer_fast(amqp_consumer_t *);


/*
 * header API
 */
int amqp_header_dec(struct _amqp_conn *, amqp_header_t **);
int amqp_header_enc(amqp_header_t *, struct _amqp_conn *);
amqp_header_t *amqp_header_new(void);
void amqp_header_destroy(amqp_header_t **);
void amqp_header_dump(amqp_header_t *);

#define AMQP_HEADER_SET_REF(n) amqp_header_set_##n
#define AMQP_HEADER_SETH_REF(n) amqp_header_set_headers_add_##n

#define AMQP_HEADER_SET_DECL(n, ty)                            \
void AMQP_HEADER_SET_REF(n)(amqp_header_t *header, ty v)       \


#define AMQP_HEADER_SETH_DECL(n, ty)                                           \
void AMQP_HEADER_SETH_REF(n)(amqp_header_t *header, const char *key, ty val)   \


AMQP_HEADER_SET_DECL(content_type, bytes_t *);
AMQP_HEADER_SET_DECL(content_encoding, bytes_t *);

AMQP_HEADER_SETH_DECL(boolean, char);
AMQP_HEADER_SETH_DECL(i8, int8_t);
AMQP_HEADER_SETH_DECL(u8, uint8_t);
AMQP_HEADER_SETH_DECL(i16, int16_t);
AMQP_HEADER_SETH_DECL(u16, uint16_t);
AMQP_HEADER_SETH_DECL(i32, int32_t);
AMQP_HEADER_SETH_DECL(u32, uint32_t);
AMQP_HEADER_SETH_DECL(i64, int64_t);
AMQP_HEADER_SETH_DECL(u64, uint64_t);
AMQP_HEADER_SETH_DECL(float, float);
AMQP_HEADER_SETH_DECL(double, double);
AMQP_HEADER_SETH_DECL(sstr, bytes_t *);
AMQP_HEADER_SETH_DECL(lstr, bytes_t *);

AMQP_HEADER_SET_DECL(delivery_mode, uint8_t);
AMQP_HEADER_SET_DECL(priority, uint8_t);
AMQP_HEADER_SET_DECL(correlation_id, bytes_t *);
AMQP_HEADER_SET_DECL(reply_to, bytes_t *);
AMQP_HEADER_SET_DECL(expiration, bytes_t *);
AMQP_HEADER_SET_DECL(message_id, bytes_t *);
AMQP_HEADER_SET_DECL(timestamp, uint64_t);
AMQP_HEADER_SET_DECL(type, bytes_t *);
AMQP_HEADER_SET_DECL(user_id, bytes_t *);
AMQP_HEADER_SET_DECL(app_id, bytes_t *);
AMQP_HEADER_SET_DECL(cluster_id, bytes_t *);


#define MRKAMQP_STOP_THREADS (-128)
#define MRKAMQP_PROTOCOL_ERROR (-129)
#define MRKAMQP_CONSUME_NACK (-130)
/*
 * rpc
 */
amqp_rpc_t *amqp_rpc_new(char *, char *, char *);
void amqp_rpc_destroy(amqp_rpc_t **);
MRKAMQP_SYNC int amqp_rpc_setup_client(amqp_rpc_t *, amqp_channel_t *);
MRKAMQP_SYNC int amqp_rpc_setup_server(amqp_rpc_t *,
                                       amqp_channel_t *,
                                       amqp_rpc_server_handler_t,
                                       void *);
int amqp_rpc_run(amqp_rpc_t *);
mrkthr_ctx_t *amqp_rpc_run_spawn(amqp_rpc_t *);
MRKAMQP_SYNC int amqp_rpc_teardown(amqp_rpc_t *);
MRKAMQP_SYNC int amqp_rpc_call(amqp_rpc_t *,
                  const char *,
                  size_t,
                  amqp_rpc_request_header_cb_t,
                  amqp_consumer_content_cb_t,
                  void *);

/*
 * module
 */
void mrkamqp_init(void);
void mrkamqp_fini(void);

const char *mrkamqp_diag_str(int);

#ifdef __cplusplus
}
#endif
#endif /* MRKAMQP_H_DEFINED */
