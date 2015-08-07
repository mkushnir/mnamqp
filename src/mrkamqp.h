#ifndef MRKAMQP_H_DEFINED
#define MRKAMQP_H_DEFINED

#include <mrkcommon/array.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/dict.h>

#ifdef __cplusplus
extern "C" {
#endif

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
typedef int (*amqp_decode)(struct _amqp_value *, bytestream_t *, int);

typedef struct _amqp_type {
    amqp_encode enc;
    amqp_decode dec;
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
        amqp_decimal_t dec;
        bytes_t *str;
        array_t a;
        dict_t t;
    } value;
} amqp_value_t;

#ifdef __cplusplus
}
#endif
#endif /* MRKAMQP_H_DEFINED */
