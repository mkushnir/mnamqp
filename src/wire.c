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
static amqp_type_t *amqp_type_by_tag(uint8_t);
static amqp_type_t field_types[256];



/*
 * basic types
 */

/*
 * octet
 */
void
pack_octet(bytestream_t *bs, uint8_t v)
{
    (void)bytestream_cat(bs, sizeof(uint8_t), (char *)&v);
}


ssize_t
unpack_octet(bytestream_t *bs, int fd, uint8_t *v)
{
    while (SAVAIL(bs) < (ssize_t)sizeof(uint8_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    *v = *SPDATA(bs);
    SINCR(bs);
    return 1;
}


/*
 * short
 */
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


ssize_t
unpack_short(bytestream_t *bs, int fd, uint16_t *v)
{
    union {
        char *c;
        uint16_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint16_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    u.c = SPDATA(bs);
    *v = be16toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint16_t));
    return sizeof(uint16_t);
}


/*
 * long
 */
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


ssize_t
unpack_long(bytestream_t *bs, int fd, uint32_t *v)
{
    union {
        char *c;
        uint32_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint32_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    u.c = SPDATA(bs);
    *v = be32toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint32_t));
    return sizeof(uint32_t);
}


/*
 * longlong
 */
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


ssize_t
unpack_longlong(bytestream_t *bs, int fd, uint64_t *v)
{
    union {
        char *c;
        uint64_t *i;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(uint64_t)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    u.c = SPDATA(bs);
    *v = be64toh(*u.i);
    SADVANCEPOS(bs, sizeof(uint64_t));
    return sizeof(uint64_t);
}


/*
 * float
 */
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


ssize_t
unpack_float(bytestream_t *bs, int fd, float *v)
{
    union {
        char *c;
        float *f;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(float)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    u.c = SPDATA(bs);
    *v = *u.f;
    SADVANCEPOS(bs, sizeof(float));
    return sizeof(float);
}


/*
 * double
 */
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


ssize_t
unpack_double(bytestream_t *bs, int fd, double *v)
{
    union {
        char *c;
        double *d;
    } u;

    while (SAVAIL(bs) < (ssize_t)sizeof(double)) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }
    u.c = SPDATA(bs);
    *v = *u.d;
    SADVANCEPOS(bs, sizeof(double));
    return sizeof(double);
}


/*
 * shortstr
 */
void
pack_shortstr(bytestream_t *bs, bytes_t *s)
{
    union {
        uint8_t sz;
        char c;
    } u;

    /*
     * discard terminating zero, not to be counted in AMQP
     */
    u.sz = (uint8_t)s->sz - 1;
    (void)bytestream_cat(bs, sizeof(uint8_t), &u.c);
    (void)bytestream_cat(bs, u.sz, (char *)s->data);
}


ssize_t
unpack_shortstr(bytestream_t *bs, int fd, bytes_t **v)
{
    uint8_t sz;

    if (unpack_octet(bs, fd, &sz) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    /*
     * reserve for terminating zero, not to be counted in AMQP
     */
    *v = bytes_new(sz + 1);

    while (SAVAIL(bs) < (ssize_t)sz) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }

    memcpy((*v)->data, SPDATA(bs), sz);
    (*v)->data[sz] = '\0';
    SADVANCEPOS(bs, sz);
    return sizeof(uint8_t) + sz;
}


/*
 * longstr
 */
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


ssize_t
unpack_longstr(bytestream_t *bs, int fd, bytes_t **v)
{
    uint32_t sz;

    if (unpack_long(bs, fd, &sz) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    *v = bytes_new(sz);

    while (SAVAIL(bs) < (ssize_t)sz) {
        if (bytestream_consume_data(bs, fd) != 0) {
            TRRET(UNPACK_ECONSUME);
        }
    }

    memcpy((*v)->data, SPDATA(bs), sz);
    SADVANCEPOS(bs, sz);
    return sizeof(uint32_t) + sz;
}



/*
 * field value
 */
static void
pack_field_value(bytestream_t *bs, amqp_value_t *v)
{
    pack_octet(bs, v->ty->tag);
    v->ty->enc(v, bs);
}


static ssize_t
unpack_field_value(bytestream_t *bs, int fd, amqp_value_t **v)
{
    amqp_type_t *ty;
    ssize_t res0, res1;
    uint8_t tag;

    if ((res0 = unpack_octet(bs, fd, &tag)) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    ty = amqp_type_by_tag(tag);

    if (ty->tag != tag || ty->enc == NULL) {
        TRRET(UNPACK_ECONSUME);
    }

    if (*v == NULL) {
        if ((*v = malloc(sizeof(amqp_value_t))) == NULL) {
            FAIL("malloc");
        }
    }

    (*v)->ty = ty;

    if ((res1 = (*v)->ty->dec(*v, bs, fd)) < 0) {
        TRRET(UNPACK_ECONSUME);
    }
    return res0 + res1;
}



/*
 * table
 */
static int
table_item_fini(bytes_t *key, amqp_value_t *value)
{
    BYTES_DECREF(&key);
    amqp_value_destroy(&value);
    return 0;
}

static int
pack_table_cb(UNUSED dict_t *dict,
              bytes_t *key,
              amqp_value_t *value,
              void *udata)
{
    struct {
        bytestream_t *bs;
        size_t sz;
    } *params = udata;

    pack_shortstr(params->bs, key);
    params->sz += sizeof(uint8_t) + key->sz - 1;
    pack_field_value(params->bs, value);
    params->sz += value->ty->len(value);
    return 0;
}


void
pack_table(bytestream_t *bs, dict_t *v)
{
    struct {
        bytestream_t *bs;
        size_t sz;
    } params;
    off_t seod;
    union {
        uint32_t *i;
        char *c;
    } u;

    params.bs = bs;
    params.sz = 0;
    seod = SEOD(bs);
    pack_long(bs, 0); // placeholder
    (void)dict_traverse(v,
                        (dict_traverser_t)pack_table_cb,
                        &params);
    u.c = SDATA(bs, seod);
    *u.i = htobe32((uint32_t)params.sz);
}


ssize_t
unpack_table(bytestream_t *bs, int fd, dict_t *v)
{
    uint32_t sz;
    ssize_t nread;

    if (unpack_long(bs, fd, &sz) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    dict_init(v, 17,
             (dict_hashfn_t)bytes_hash,
             (dict_item_comparator_t)bytes_cmp,
             (dict_item_finalizer_t)table_item_fini);

    //while (SAVAIL(bs) < (ssize_t)sz) {
    //    if (bytestream_consume_data(bs, fd) != 0) {
    //        TRRET(UNPACK_ECONSUME);
    //    }
    //}

    for (nread = 0; nread <= sz;) {
        ssize_t n;
        bytes_t *key;
        amqp_value_t *value;

        key = NULL;
        if ((n = unpack_shortstr(bs, fd, &key)) < 0) {
            BYTES_DECREF(&key);
            TRRET(UNPACK_ECONSUME);
        }
        nread += n;

        value = NULL;
        if ((n = unpack_field_value(bs, fd, &value)) < 0) {
            amqp_value_destroy(&value);
            TRRET(UNPACK_ECONSUME);
        }
        nread += n;

        /* ignore dups */
        if (dict_get_item(v, key) != NULL) {
            BYTES_DECREF(&key);
            amqp_value_destroy(&value);
        } else {
            dict_set_item(v, key, value);
        }
    }

    assert(nread == sz);

    return sizeof(uint32_t) + sz;
}


/*
 * array
 */
static int
pack_array_cb(amqp_value_t **v, void *udata)
{
    bytestream_t *bs;

    bs = udata;
    assert(*v != NULL);
    pack_field_value(bs, *v);
    return 0;
}

void
pack_array(bytestream_t *bs, array_t *v)
{
    pack_long(bs, (uint32_t)v->elnum);
    array_traverse(v, (array_traverser_t)pack_array_cb, bs);
}


static int
array_item_fini(amqp_value_t **v)
{
    amqp_value_destroy(v);
    return 0;
}


ssize_t
unpack_array(bytestream_t *bs, int fd, array_t *v)
{
    uint32_t elnum, i;
    ssize_t nread;

    if ((nread = unpack_long(bs, fd, &elnum)) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    array_init(v, 0, sizeof(amqp_value_t *),
               NULL,
               (array_finalizer_t)array_item_fini);

    for (i = 0; i < elnum; ++i) {
        ssize_t n;
        amqp_value_t **value;

        if ((value = array_incr(v)) == NULL) {
            FAIL("array_incr");
        }

        *value = NULL;
        if ((n = unpack_field_value(bs, fd, value)) < 0) {
            amqp_value_destroy(value);
            TRRET(UNPACK_ECONSUME);
        }
        nread += n;
    }
    return nread;
}


/*
 * field values
 */

/*
 * bool
 */
static void
enc_bool(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->value.b);
}


static ssize_t
dec_bool(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, (uint8_t *)&v->value.b);
}


/*
 * int8
 */
static size_t
len_i8(UNUSED amqp_value_t *v)
{
    return 2 * sizeof(uint8_t);
}


static void
enc_int8(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->value.i8);
}


static ssize_t
dec_int8(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, (uint8_t *)&v->value.i8);
}


/*
 * uint8
 */
static void
enc_uint8(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->value.u8);
}


static ssize_t
dec_uint8(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_octet(bs, fd, &v->value.u8);
}


/*
 * int16
 */
static void
enc_int16(amqp_value_t *v, bytestream_t *bs)
{
    pack_short(bs, v->value.i16);
}


static ssize_t
dec_int16(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_short(bs, fd, (uint16_t *)&v->value.i16);
}


/*
 * uint16
 */
static void
enc_uint16(amqp_value_t *v, bytestream_t *bs)
{
    pack_short(bs, v->value.u16);
}


static ssize_t
dec_uint16(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_short(bs, fd, &v->value.u16);
}


static size_t
len_i16(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(uint16_t);
}


/*
 * int32
 */
static void
enc_int32(amqp_value_t *v, bytestream_t *bs)
{
    pack_long(bs, v->value.i32);
}


static ssize_t
dec_int32(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_long(bs, fd, (uint32_t *)&v->value.i32);
}


static size_t
len_i32(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(uint32_t);
}


/*
 * uint32
 */
static void
enc_uint32(amqp_value_t *v, bytestream_t *bs)
{
    pack_long(bs, v->value.u32);
}


static ssize_t
dec_uint32(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_long(bs, fd, &v->value.u32);
}


/*
 * int64
 */
static void
enc_int64(amqp_value_t *v, bytestream_t *bs)
{
    pack_longlong(bs, v->value.i64);
}


static ssize_t
dec_int64(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longlong(bs, fd, (uint64_t *)&v->value.i64);
}


static size_t
len_i64(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(uint64_t);
}


/*
 * uint64
 */
static void
enc_uint64(amqp_value_t *v, bytestream_t *bs)
{
    pack_longlong(bs, v->value.u64);
}


static ssize_t
dec_uint64(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longlong(bs, fd, &v->value.u64);
}


/*
 * float
 */
static void
enc_float(amqp_value_t *v, bytestream_t *bs)
{
    pack_float(bs, v->value.f);
}


static ssize_t
dec_float(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_float(bs, fd, &v->value.f);
}


static size_t
len_float(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(float);
}


/*
 * double
 */
static void
enc_double(amqp_value_t *v, bytestream_t *bs)
{
    pack_double(bs, v->value.f);
}


static ssize_t
dec_double(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_double(bs, fd, &v->value.d);
}


static size_t
len_double(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(double);
}


/*
 * decimal
 */
static void
enc_decimal(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->value.dc.places);
    pack_long(bs, v->value.dc.value);
}


static ssize_t
dec_decimal(amqp_value_t *v, bytestream_t *bs, int fd)
{
    ssize_t res0, res1;

    if ((res0 = unpack_octet(bs, fd, &v->value.dc.places)) < 0) {
        TRRET(UNPACK_ECONSUME);
    }
    if ((res1 = unpack_long(bs, fd, (uint32_t *)&v->value.dc.value)) < 0) {
        TRRET(UNPACK_ECONSUME);
    }

    return res0 + res1;
}


static size_t
len_decimal(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t);
}


/*
 * sstr
 */
static void
enc_sstr(amqp_value_t *v, bytestream_t *bs)
{
    pack_shortstr(bs, v->value.str);
}


static ssize_t
dec_sstr(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_shortstr(bs, fd, &v->value.str);
}


static size_t
len_sstr(amqp_value_t *v)
{
    /*
     * compansate terminating zero, see unpack_shortstr()
     */
    return sizeof(uint8_t) + sizeof(uint8_t) + v->value.str->sz - 1;
}


/*
 * lstr
 */
static void
enc_lstr(amqp_value_t *v, bytestream_t *bs)
{
    pack_longstr(bs, v->value.str);
}


static ssize_t
dec_lstr(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_longstr(bs, fd, &v->value.str);
}


static size_t
len_lstr(amqp_value_t *v)
{
    return sizeof(uint8_t) + sizeof(uint32_t) + v->value.str->sz;
}


static void
kill_str(amqp_value_t *v)
{
    BYTES_DECREF(&v->value.str);
}


/*
 * array
 */
static void
enc_array(UNUSED amqp_value_t *v, UNUSED bytestream_t *bs)
{
}


static ssize_t
dec_array(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_array(bs, fd, &v->value.a);
}


static int
len_array_cb(amqp_value_t **v, size_t *sz)
{
    *sz += (*v)->ty->len(*v);
    return 0;
}


static size_t
len_array(amqp_value_t *v)
{
    size_t sz;

    sz = 0;
    array_traverse(&v->value.a, (array_traverser_t)len_array_cb, &sz);
    return sizeof(uint8_t) + sizeof(uint32_t) + sz;
}


static void
kill_array(amqp_value_t *v)
{
    array_fini(&v->value.a);
}


/*
 * table
 */
static void
enc_table(amqp_value_t *v, bytestream_t *bs)
{
    pack_table(bs, &v->value.t);
}


static ssize_t
dec_table(amqp_value_t *v, bytestream_t *bs, int fd)
{
    return unpack_table(bs, fd, &v->value.t);
}


static int
len_table_cb(amqp_value_t *key, amqp_value_t *val, size_t *sz)
{
    (*sz) += key->ty->len(key) + val->ty->len(val);
    return 0;
}


static size_t
len_table(amqp_value_t *v)
{
    size_t sz;

    sz = 0;
    dict_traverse(&v->value.t, (dict_traverser_t)len_table_cb, &sz);
    return sizeof(uint8_t) + sizeof(uint32_t) + sz;
}


static void
kill_table(amqp_value_t *v)
{
    dict_fini(&v->value.t);
}


/*
 * void
 */
static void
enc_void(amqp_value_t *v, bytestream_t *bs)
{
    pack_octet(bs, v->ty->tag);
}


static ssize_t
dec_void(UNUSED amqp_value_t *v, UNUSED bytestream_t *bs, UNUSED int fd)
{
    return 0;
}


static size_t
len_void(UNUSED amqp_value_t *v)
{
    return sizeof(uint8_t);
}


static struct {
    char tag;
    amqp_encode enc;
    amqp_decode dec;
    amqp_len len;
    amqp_kill kill;
} _typeinfo[] = {
    {AMQP_TBOOL, enc_bool, dec_bool, len_i8, NULL},
    {AMQP_TINT8, enc_int8, dec_int8, len_i8, NULL},
    {AMQP_TUINT8, enc_uint8, dec_uint8, len_i8, NULL},
    {AMQP_TINT16, enc_int16, dec_int16, len_i16, NULL},
    {AMQP_TUINT16, enc_uint16, dec_uint16, len_i16, NULL},
    {AMQP_TINT32, enc_int32, dec_int32, len_i32, NULL},
    {AMQP_TUINT32, enc_uint32, dec_uint32, len_i32, NULL},
    {AMQP_TINT64, enc_int64, dec_int64, len_i64, NULL},
    {AMQP_TUINT64, enc_uint64, dec_uint64, len_i64, NULL},
    {AMQP_TFLOAT, enc_float, dec_float, len_float, NULL},
    {AMQP_TDOUBLE, enc_double, dec_double, len_double, NULL},
    {AMQP_TDECIMAL, enc_decimal, dec_decimal, len_decimal, NULL},
    {AMQP_TSSTR, enc_sstr, dec_sstr, len_sstr, kill_str},
    {AMQP_TLSTR, enc_lstr, dec_lstr, len_lstr, kill_str},
    {AMQP_TARRAY, enc_array, dec_array, len_array, kill_array},
    {AMQP_TTSTAMP, enc_int64, dec_int64, len_i64, NULL},
    {AMQP_TTABLE, enc_table, dec_table, len_table, kill_table},
    {AMQP_TVOID, enc_void, dec_void, len_void, NULL},
};


static amqp_type_t *
amqp_type_by_tag(uint8_t tag)
{
    assert(field_types[tag].enc != NULL);
    return &field_types[tag];
}


int
amqp_decode_table(bytestream_t *bs, int fd, amqp_value_t **v)
{
    if (*v == NULL) {
        if ((*v = malloc(sizeof(amqp_value_t))) == NULL) {
            FAIL("malloc");
        }
    }

    (*v)->ty = amqp_type_by_tag(AMQP_TTABLE);

    if ((*v)->ty->dec(*v, bs, fd) < 0) {
        TRRET(AMQP_DECODE_TABLE + 1);
    }
    return 0;
}


void
amqp_value_destroy(amqp_value_t **v)
{
    if (*v != NULL) {
        if ((*v)->ty->kill != NULL) {
            (*v)->ty->kill(*v);
        }
        free(*v);
        *v = NULL;
    }
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
        ty->len = _typeinfo[i].len;
    }
}


void
mrkamqp_fini(void)
{
}
