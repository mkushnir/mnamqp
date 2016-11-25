#include "mygauge.h"

void
mygauge_init(mygauge_t *g, long i)
{
    g->g0 = i;
    g->g1 = i;
}


void
mygauge_update(mygauge_t *g, long i)
{
    g->g1 = i;
}


void
mygauge_incr(mygauge_t *g, long i)
{
    g->g1 += i;
}


long
mygauge_diff(mygauge_t *g)
{
    return g->g1 - g->g0;
}


long
mygauge_flush(mygauge_t *g)
{
    long diff = g->g1 - g->g0;
    g->g0 = g->g1;
    return diff;
}


