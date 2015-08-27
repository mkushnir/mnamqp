#ifndef MRKAMQP_H_DEFINED
#define MRKAMQP_H_DEFINED

#include <mrkcommon/array.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/dict.h>
#include <mrkcommon/stqueue.h>

#include <mrkthr.h>

#ifdef __cplusplus
extern "C" {
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
    uint64_t since_last_frame;

    int fd;
    bytestream_t ins;
    bytestream_t outs;
    mrkthr_ctx_t *recv_thread;
    mrkthr_ctx_t *send_thread;
    /* outgoing frames */
    STQUEUE(_amqp_frame, oframes);
    mrkthr_signal_t oframe_sig;

    array_t channels;
    struct _amqp_channel *chan0;
    int closed:1;
} amqp_conn_t;


typedef struct _amqp_pending_pub {
    STQUEUE_ENTRY(_amqp_pending_pub, link);
    mrkthr_signal_t sig;
    uint64_t publish_tag;
} amqp_pending_pub_t;


typedef struct _amqp_channel {
    amqp_conn_t *conn;
    /* incoming frames */
    STQUEUE(_amqp_frame, iframes);
    mrkthr_signal_t iframe_sig;
    dict_t consumers;
    /* weak ref */
    struct _amqp_consumer *content_consumer;
    uint64_t publish_tag;
    STQUEUE(_amqp_pending_pub, pending_pub);
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

typedef void (*amqp_consumer_content_cb_t)(amqp_frame_t *,
                                           amqp_frame_t *,
                                           char *,
                                           void *);
typedef struct _amqp_consumer {
    amqp_channel_t *chan;
    bytes_t *consumer_tag;
    //amqp_frame_t *content_method;
    //amqp_frame_t *content_header;
    //STQUEUE(_amqp_frame, content_body);
    STQUEUE(_amqp_pending_content, pending_content);
    mrkthr_signal_t content_sig;
    mrkthr_ctx_t *content_thread;
    amqp_consumer_content_cb_t content_cb;
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
    dict_t headers;
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

    uint64_t received_size;
} amqp_header_t;


/*
 * rpc
 */
typedef void (*amqp_rpc_server_handler_t)(const amqp_header_t *,
                                          const char *,
                                          amqp_header_t **,
                                          char **,
                                          void *);
typedef struct _amqp_rpc {
    char *exchange;
    char *routing_key;
    bytes_t *reply_to;
    /* weakref */
    amqp_channel_t *chan;
    amqp_consumer_t *cons;
    /* key weakref, value weakref */
    dict_t calls;
    uint64_t next_id;
    amqp_consumer_content_cb_t cb;
    amqp_rpc_server_handler_t server_handler;
    void *server_udata;
} amqp_rpc_t;


/*
 * conn
 */
amqp_conn_t *amqp_conn_new(const char *,
                           int,
                           const char *,
                           const char *,
                           const char *,
                           short,
                           int,
                           short);
void amqp_conn_destroy(amqp_conn_t **);
int amqp_conn_open(amqp_conn_t *);
int amqp_conn_run(amqp_conn_t *);
int amqp_conn_close(amqp_conn_t *);


/*
 * channel
 */
amqp_channel_t *amqp_create_channel(amqp_conn_t *);
#define CHANNEL_CONFIRM_FNOWAIT         0x01
int amqp_channel_confirm(amqp_channel_t *, uint8_t);
int amqp_close_channel(amqp_channel_t *);
#define DECLARE_EXCHANGE_FPASSIVE       0x01
#define DECLARE_EXCHANGE_FDURABLE       0x02
#define DECLARE_EXCHANGE_FAUTODELETE    0x04
#define DECLARE_EXCHANGE_FINTERNAL      0x08
#define DECLARE_EXCHANGE_FNOWAIT        0x10
int amqp_channel_declare_exchange(amqp_channel_t *,
                                  const char *,
                                  const char *,
                                  uint8_t);
#define DELETE_EXCHANGE_FIFUNUSED       0x01
#define DELETE_EXCHANGE_FNOWAIT         0x02
int amqp_channel_delete_exchange(amqp_channel_t *,
                                 const char *,
                                 uint8_t);

#define DECLARE_QUEUE_FPASSIVE          0x01
#define DECLARE_QUEUE_FDURABLE          0x02
#define DECLARE_QUEUE_FEXCLUSIVE        0x04
#define DECLARE_QUEUE_FAUTODELETE       0x08
#define DECLARE_QUEUE_FNOWAIT           0x10
int amqp_channel_declare_queue(amqp_channel_t *,
                               const char *,
                               uint8_t);
#define BIND_QUEUE_FNOWAIT              0x01
int amqp_channel_bind_queue(amqp_channel_t *,
                            const char *,
                            const char *,
                            const char *,
                            uint8_t);
int amqp_channel_unbind_queue(amqp_channel_t *,
                              const char *,
                              const char *,
                              const char *);
#define PURGE_QUEUE_FNOWAIT             0x01
int amqp_channel_purge_queue(amqp_channel_t *,
                             const char *,
                             uint8_t);
#define DELETE_QUEUE_FIFUNUSED          0x01
#define DELETE_QUEUE_FIFEMPTY           0x02
#define DELETE_QUEUE_FNOWAIT            0x04
int amqp_channel_delete_queue(amqp_channel_t *,
                              const char *,
                              uint8_t);
#define QOS_FGLOBAL                     0x01
int amqp_channel_qos(amqp_channel_t *,
                     uint32_t,
                     uint16_t,
                     uint8_t);
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
int amqp_channel_publish(amqp_channel_t *,
                         const char *,
                         const char *,
                         uint8_t,
                         amqp_header_completion_cb,
                         void *,
                         const char *,
                         ssize_t);
int amqp_channel_publish_ex(amqp_channel_t *,
                            const char *,
                            const char *,
                            uint8_t,
                            amqp_header_t *,
                            const char *);

#define ACK_MULTIPLE                    0x01

/*
 * consumer
 */
#define CONSUME_FNOLOCAL                0x01
#define CONSUME_FNOACK                  0x02
#define CONSUME_FEXCLUSIVE              0x04
#define CONSUME_FNOWAIT                 0x08
amqp_consumer_t *amqp_channel_create_consumer(amqp_channel_t *,
                                              const char *,
                                              const char *,
                                              uint8_t);
int amqp_consumer_handle_content_spawn(amqp_consumer_t *,
                                       amqp_consumer_content_cb_t,
                                       void *);
int amqp_consumer_handle_content(amqp_consumer_t *,
                                 amqp_consumer_content_cb_t,
                                 void *);
amqp_frame_t *amqp_consumer_get_method(amqp_consumer_t *);
amqp_frame_t *amqp_consumer_get_header(amqp_consumer_t *);
amqp_frame_t *amqp_consumer_get_body(amqp_consumer_t *);
void amqp_consumer_reset_content_state(amqp_consumer_t *);
int amqp_close_consumer(amqp_consumer_t *);


/*
 * header API
 */
int amqp_header_dec(struct _amqp_conn *, amqp_header_t **);
int amqp_header_enc(amqp_header_t *, struct _amqp_conn *);
amqp_header_t *amqp_header_new(void);
void amqp_header_destroy(amqp_header_t **);
void amqp_header_dump(amqp_header_t *);

#define AMQP_HEADER_SET_REF(n) amqp_header_set_##n

#define AMQP_HEADER_SET_DECL(n, ty)                            \
void AMQP_HEADER_SET_REF(n)(amqp_header_t *header, ty v)       \

AMQP_HEADER_SET_DECL(content_type, bytes_t *);
AMQP_HEADER_SET_DECL(content_encoding, bytes_t *);
void amqp_header_set_headers(amqp_header_t *, bytes_t *, amqp_value_t *);
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


/*
 * rpc
 */
amqp_rpc_t *amqp_rpc_new(char *, char *, char *);
void amqp_rpc_destroy(amqp_rpc_t **);
int amqp_rpc_setup_client(amqp_rpc_t *, amqp_channel_t *);
int amqp_rpc_setup_server(amqp_rpc_t *,
                          amqp_channel_t *,
                          amqp_rpc_server_handler_t,
                          void *);
int amqp_rpc_run(amqp_rpc_t *);
int amqp_rpc_teardown(amqp_rpc_t *);
int amqp_rpc_call(amqp_rpc_t *, bytes_t *, char **, size_t *);

/*
 * module
 */
void mrkamqp_init(void);
void mrkamqp_fini(void);


#ifdef __cplusplus
}
#endif
#endif /* MRKAMQP_H_DEFINED */
