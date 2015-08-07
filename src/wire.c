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
//#include <arpa/inet.h>
#include <string.h>

#include <mrkcommon/dict.h>
#include <mrkcommon/bytestream.h>
#include <mrkcommon/bytes.h>
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkamqp.h>

#include "diag.h"


/*
 * field types
 */
static amqp_type_t field_types[256];



/*
 * basic types
 */
void
pack_octet(bytestream_t *bs, uint8_t v)
{
    (void)bytestream_cat(bs, sizeof(uint8_t), (char *)&v);
}


int
unpack_octet(bytestream_t *bs, int fd, uint8_t *v)
{
    while (SAVAIL(bs) < (ssize_t)sizeof(uint8_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 1;
        }
    }
    *v = *SPDATA(bs);
    SINCR(bs);
    return 0;
}


void
pack_short(bytestream_t *bs, uint16_t v)
{
    union {
        uint16_t i;
        char c[sizeof(uint16_t)];
    } u;

    u.i = htobe16(v);
    (void)bytestream_cat(bs, sizeof(uint16_t), u.c);
}


int
unpack_short(bytestream_t *bs, int fd, uint16_t *v)
{
    union {
        char *c;
        uint16_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint16_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 2;
        }
    }
    u.c = SPDATA(bs);
    *v = be16toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint16_t));
    return 0;
}


void
pack_long(bytestream_t *bs, uint32_t v)
{
    union {
        uint32_t i;
        char c[sizeof(uint32_t)];
    } u;

    u.i = htobe32(v);
    (void)bytestream_cat(bs, sizeof(uint32_t), u.c);
}


int
unpack_long(bytestream_t *bs, int fd, uint32_t *v)
{
    union {
        char *c;
        uint32_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint32_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 3;
        }
    }
    u.c = SPDATA(bs);
    *v = be32toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint32_t));
    return 0;
}


void
pack_longlong(bytestream_t *bs, uint64_t v)
{
    union {
        uint64_t i;
        char c[sizeof(uint64_t)];
    } u;

    u.i = htobe64(v);
    (void)bytestream_cat(bs, sizeof(uint64_t), u.c);
}


int
unpack_longlong(bytestream_t *bs, int fd, uint64_t *v)
{
    union {
        char *c;
        uint64_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint64_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 4;
        }
    }
    u.c = SPDATA(bs);
    *v = be64toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint64_t));
    return 0;
}


void
pack_float(bytestream_t *bs, float v)
{
    union {
        float f;
        char c[sizeof(uint64_t)];
    } u;

    u.f = v;
    (void)bytestream_cat(bs, sizeof(float), u.c);
}


int
unpack_float(bytestream_t *bs, int fd, float *v)
{
    union {
        char *c;
        float *f;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(float)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 5;
        }
    }
    u.c = SPDATA(bs);
    *v = *u.f;
    SADVANCEPOS(bs, sizeof(float));
    return 0;
}


void
pack_double(bytestream_t *bs, double v)
{
    union {
        double d;
        char c[sizeof(uint64_t)];
    } u;

    u.d = v;
    (void)bytestream_cat(bs, sizeof(double), u.c);
}


int
unpack_double(bytestream_t *bs, int fd, double *v)
{
    union {
        char *c;
        double *d;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(double)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 6;
        }
    }
    u.c = SPDATA(bs);
    *v = *u.d;
    SADVANCEPOS(bs, sizeof(double));
    return 0;
}


void
pack_shortstr(bytestream_t *bs, bytes_t *s)
{
    union {
        uint8_t sz;
        char c;
    } u;

    u.sz = (uint8_t)s->sz;
    (void)bytestream_cat(bs, sizeof(uint8_t), &u.c);
    (void)bytestream_cat(bs, u.sz, (char *)s->data);
}


int
unpack_shortstr(bytestream_t *bs, int fd, bytes_t **v)
{
    uint8_t sz;

    if (unpack_octet(bs, fd, &sz) != 0) {
        return UNPACK + 7;
    }

    *v = bytes_new(sz + 1);

    while (SAVAIL(bs) < (ssize_t)sz) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 8;
        }
    }

    memcpy((*v)->data, SPDATA(bs), sz);
    (*v)->data[sz] = '\0';
    SADVANCEPOS(bs, sz);
    return 0;
}


void
pack_longstr(bytestream_t *bs, bytes_t *s)
{
    union {
        uint32_t sz;
        char c;
    } u;

    u.sz = (uint32_t)s->sz;
    (void)bytestream_cat(bs, sizeof(uint32_t), &u.c);
    (void)bytestream_cat(bs, u.sz, (char *)s->data);
}


int
unpack_longstr(bytestream_t *bs, int fd, bytes_t **v)
{
    uint32_t sz;

    if (unpack_long(bs, fd, &sz) != 0) {
        return UNPACK + 9;
    }

    *v = bytes_new(sz);

    while (SAVAIL(bs) < (ssize_t)sz) {
        if (bytestream_consume_data(bs, fd) != 0) {
            return UNPACK + 10;
        }
    }

    memcpy((*v)->data, SPDATA(bs), sz);
    SADVANCEPOS(bs, sz);
    return 0;
}


//static int
//pack_table_cb(dict_t *dict, bytes_t )
//
//void
//pack_table(bytestream_t *bs, dict_t *dict)
//{
//    off_t start;
//    struct {} params;
//
//    start = SEOD(bs);
//    (void)dict_traverse();
//}


/*
 * field values
 */
static void
enc_bool(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_octet(bs, v->value.b);
}


static int
dec_bool(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, (uint8_t *)&v->value.b);
}


static void
enc_int8(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_octet(bs, v->value.i8);
}


static int
dec_int8(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, (uint8_t *)&v->value.i8);
}


static void
enc_uint8(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_octet(bs, v->value.u8);
}


static int
dec_uint8(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, &v->value.u8);
}


static void
enc_int16(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_short(bs, v->value.i16);
}


static int
dec_int16(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_short(bs, fd, (uint16_t *)&v->value.i16);
}


static void
enc_uint16(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_short(bs, v->value.u16);
}


static int
dec_uint16(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_short(bs, fd, &v->value.u16);
}


static void
enc_int32(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_long(bs, v->value.i32);
}


static int
dec_int32(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_long(bs, fd, (uint32_t *)&v->value.i32);
}


static void
enc_uint32(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_long(bs, v->value.u32);
}


static int
dec_uint32(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_long(bs, fd, &v->value.u32);
}


static void
enc_int64(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_longlong(bs, v->value.i64);
}


static int
dec_int64(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longlong(bs, fd, (uint64_t *)&v->value.i64);
}


static void
enc_uint64(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_longlong(bs, v->value.u64);
}


static int
dec_uint64(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longlong(bs, fd, &v->value.u64);
}


static void
enc_float(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_float(bs, v->value.f);
}


static int
dec_float(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_float(bs, fd, &v->value.f);
}


static void
enc_double(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_double(bs, v->value.f);
}


static int
dec_double(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_double(bs, fd, &v->value.d);
}


static void
enc_decimal(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_octet(bs, v->value.dec.places);
    pack_long(bs, v->value.dec.value);
}


static int
dec_decimal(amqp_value_t *v, bytestream_t *bs, int fd)
{
    if (unpack_octet(bs, fd, &v->value.dec.places) != 0) {
        return UNPACK + 11;
    }
    return unpack_long(bs, fd, (uint32_t *)&v->value.dec.value);
}


static void
enc_sstr(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_shortstr(bs, v->value.str);
}


static int
dec_sstr(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_shortstr(bs, fd, &v->value.str);
}


static void
enc_lstr(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
    pack_longstr(bs, v->value.str);
}


static int
dec_lstr(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longstr(bs, fd, &v->value.str);
}


static void
enc_array(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
}


static int
dec_array(UNUSED amqp_value_t *v, UNUSED bytestream_t *bs, UNUSED int fd)
{
    return 0;
}


static void
enc_table(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
}


static int
dec_table(UNUSED amqp_value_t *v, UNUSED bytestream_t *bs, UNUSED int fd)
{
    return 0;
}


static void
enc_void(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
}


static int
dec_void(UNUSED amqp_value_t *v, UNUSED bytestream_t *bs, UNUSED int fd)
{
    return 0;
}


static struct {
    char tag;
    amqp_encode enc;
    amqp_decode dec;
} _typeinfo[] = {
    {AMQP_TBOOL, enc_bool, dec_bool},
    {AMQP_TINT8, enc_int8, dec_int8},
    {AMQP_TUINT8, enc_uint8, dec_uint8},
    {AMQP_TINT16, enc_int16, dec_int16},
    {AMQP_TUINT16, enc_uint16, dec_uint16},
    {AMQP_TINT32, enc_int32, dec_int32},
    {AMQP_TUINT32, enc_uint32, dec_uint32},
    {AMQP_TINT64, enc_int64, dec_int64},
    {AMQP_TUINT64, enc_uint64, dec_uint64},
    {AMQP_TFLOAT, enc_float, dec_float},
    {AMQP_TDOUBLE, enc_double, dec_double},
    {AMQP_TDECIMAL, enc_decimal, dec_decimal},
    {AMQP_TSSTR, enc_sstr, dec_sstr},
    {AMQP_TLSTR, enc_lstr, dec_lstr},
    {AMQP_TARRAY, enc_array, dec_array},
    {AMQP_TTSTAMP, enc_int64, dec_int64},
    {AMQP_TTABLE, enc_table, dec_table},
    {AMQP_TVOID, enc_void, dec_void},
};


amqp_type_t *
amqp_type_by_tag(uint8_t tag)
{
    return &field_types[tag];
}


int
amqp_decode_field_value(bytestream_t *bs, int fd, amqp_value_t **v)
{
    uint8_t tag;

    if (*v == NULL) {
        if ((*v = malloc(sizeof(amqp_value_t))) == NULL) {
            FAIL("malloc");
        }
    }
    if (unpack_octet(bs, fd, &tag) != 0) {
        return UNPACK + 12;
    }

    (*v)->ty = amqp_type_by_tag(tag);

    if ((*v)->ty->dec(*v, bs, fd) != 0) {
        return UNPACK + 13;
    }
    return 0;
}


void
mrkamqp_init(void)
{
    size_t i;

    for (i = 0; i < countof(_typeinfo); ++i) {
        amqp_type_t *ty;

        ty = field_types + _typeinfo[i].tag;
        ty->tag = _typeinfo[i].tag;
        ty->enc = _typeinfo[i].enc;
        ty->dec = _typeinfo[i].dec;
    }
}


void
mrkamqp_fini(void)
{
}
