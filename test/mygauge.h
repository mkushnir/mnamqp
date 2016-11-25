#ifndef MYGAUGE_H_DEFINED
#define MYGAUGE_H_DEFINED

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _mygauge {
    long g0;
    long g1;
} mygauge_t;

void mygauge_init(mygauge_t *, long);
void mygauge_update(mygauge_t *, long);
void mygauge_incr(mygauge_t *, long);
long mygauge_diff(mygauge_t *);
long mygauge_flush(mygauge_t *);

#ifdef __cplusplus
}
#endif
#endif
