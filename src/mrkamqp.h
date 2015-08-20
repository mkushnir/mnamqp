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
    int heartbeat;
    int frame_max;
    uint64_t since_last_frame;

    int fd;
    bytestream_t ins;
    bytestream_t outs;
    mrkthr_ctx_t *recv_thread;

    array_t channels;
    struct _amqp_channel *chan0;
} amqp_conn_t;


struct _amqp_consumer;
typedef struct _amqp_channel {
    amqp_conn_t *conn;
    STQUEUE(_amqp_frame, frames);
    mrkthr_signal_t recvsig;
    dict_t consumers;
    struct _amqp_consumer *content_consumer;
    int id;
    int confirm_mode:1;
    int closed:1;
} amqp_channel_t;


typedef void (*amqp_consumer_content_cb_t)(amqp_frame_t *,
                                           amqp_frame_t *,
                                           char *,
                                           void *);
typedef struct _amqp_consumer {
    amqp_channel_t *chan;
    bytes_t *consumer_tag;
    amqp_frame_t *content_method;
    amqp_frame_t *content_header;
    STQUEUE(_amqp_frame, content_body);
    mrkthr_signal_t recvsig;
    mrkthr_ctx_t *content_thread;
    amqp_consumer_content_cb_t content_cb;
    void *content_udata;
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
                           int,
                           int);
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
#define CONSUME_FNOLOCAL                0x01
#define CONSUME_FNOACK                  0x02
#define CONSUME_FEXCLUSIVE              0x04
#define CONSUME_FNOWAIT                 0x08
int amqp_channel_consume(amqp_channel_t *,
                         const char *,
                         const char *,
                         uint8_t);
#define CANCEL_FNOWAIT                  0x01
int amqp_channel_cancel(amqp_channel_t *,
                        const char *,
                        uint8_t);


/*
 * consumer
 */
amqp_consumer_t *amqp_channel_create_consumer(amqp_channel_t *,
                                              const char *,
                                              const char *);
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
