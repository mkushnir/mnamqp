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

typedef struct _amqp_conn {
    char *host;
    int port;
    char *user;
    char *password;
    char *vhost;
    uint16_t channel_max;
    uint32_t frame_max;
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
} amqp_conn_t;


typedef struct _amqp_pending_pub {
    STQUEUE_ENTRY(_amqp_pending_pub, link);
    mrkthr_signal_t sig;
    uint64_t publish_tag;
} amqp_pending_pub_t;


struct _amqp_consumer;
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
int amqp_channel_publish(amqp_channel_t *,
                         const char *,
                         const char *,
                         uint8_t,
                         const char *,
                         ssize_t);

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
void amqp_consumer_handle_content(amqp_consumer_t *,
                                  amqp_consumer_content_cb_t,
                                  void *);
amqp_frame_t *amqp_consumer_get_method(amqp_consumer_t *);
amqp_frame_t *amqp_consumer_get_header(amqp_consumer_t *);
amqp_frame_t *amqp_consumer_get_body(amqp_consumer_t *);
void amqp_consumer_reset_content_state(amqp_consumer_t *);
int amqp_close_consumer(amqp_consumer_t *);


/*
 * module
 */
void mrkamqp_init(void);
void mrkamqp_fini(void);


#ifdef __cplusplus
}
#endif
#endif /* MRKAMQP_H_DEFINED */
