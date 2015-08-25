#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>

#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>
#include <mrkcommon/traversedir.h>

#include <mrkthr.h>

#include <mrkamqp_private.h>


#include "diag.h"

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif


static char *src = NULL;
static char *dest = NULL;
//static char *dir = NULL;

static amqp_conn_t *conn = NULL;

static int shutting_down = 0;

static void
_shutdown(void)
{
    mrkamqp_fini();
    {
        char *s[] = {src, dest};
        unsigned i;

        for (i = 0; i < countof(s); ++i) {
            if (s[i] != NULL) {
                free(s[i]);
            }
        }
    }
    mrkthr_shutdown();
}


static int
sigshutdown(UNUSED int argc, UNUSED void **argv)
{
    if (!shutting_down) {
        shutting_down = 1;
        amqp_conn_close(conn);
        amqp_conn_destroy(&conn);
        _shutdown();
        TRACE("Shutting down. Another signal will cause immediate exit.");
    } else {
        TRACE("Exiting (sigshutdown)...");
        exit(0);
    }
    return 0;
}


static void
myterm(UNUSED int sig)
{
    (void)mrkthr_spawn("sigshutdown", sigshutdown, 0);
}


void usage(char *path)
{
    printf("Usage: %s "
           "[ -h ] "
           "-s SRC "
           "-d DST "
           "[ PATH ,,,] "
           "\n", basename(path));
}


static int
traversedir_cb(const char *dir, struct dirent *de, UNUSED void *udata)
{
    char *path;

    if (de != NULL) {
        path = path_join(dir, de->d_name);
        CTRACE("file=%s", path);
        free(path);
    }
    return 0;
}


UNUSED static void
run1(char *path)
{
    struct stat sb;

    CTRACE("path=%s", path);
    if (stat(path, &sb) != 0) {
        perror("stat");
        return;
    }

    if (S_ISDIR(sb.st_mode)) {
        if (traverse_dir(path, traversedir_cb, NULL) != 0) {
            CTRACE("traverse_dir error");
        }
    } else {
        CTRACE("file=%s", path);
    }

}


static int
run0(UNUSED int argc, void **argv)
{
    int res;
    int _argc;
    char **_argv;
    UNUSED int i;

    UNUSED amqp_channel_t *chan;
    UNUSED amqp_consumer_t *cons;

    assert(argc == 2);

    res = 0;

    _argc = (intptr_t)argv[0];
    _argv = argv[1];

    mrkamqp_init();

    conn = amqp_conn_new("localhost",
                         5672,
                         "guest",
                         "guest",
                         "/",
                         0,
                         0,
                         0);


    if (amqp_conn_open(conn) != 0) {
        res = 1;
        goto err0;
    }

    if (amqp_conn_run(conn) != 0) {
        res = 1;
        goto err;
    }

    if ((chan = amqp_create_channel(conn)) == NULL) {
        res = 1;
        goto err;
    }

    if (amqp_channel_confirm(chan, 0) != 0) {
        res = 1;
        goto err;
    }

    while (1) {
        CTRACE();
        mrkthr_sleep(2000);
    }

    //if (amqp_channel_declare_queue_ex(chan,
    //                                  "qwe",
    //                                  DECLARE_QUEUE_FEXCLUSIVE,
    //                                  declare_queue_cb,
    //                                  NULL) != 0) {
    //    res = 1;
    //    goto err;
    //}

    //if ((cons = amqp_channel_create_consumer(chan,
    //                                         "qwe",
    //                                         NULL,
    //                                         CONSUME_FNOACK & 0)) == NULL) {
    //    res = 1;
    //    goto err;
    //}

    //for (i = 0; i < _argc; ++i) {
    //    run1(_argv[i]);
    //}

end:
    amqp_conn_close(conn);
end0:
    amqp_conn_destroy(&conn);
    return res;

err:
    TR(res);
    goto end;

err0:
    TR(res);
    goto end0;
}


int
main(int argc, char **argv)
{
    int ch;

    if (signal(SIGINT, myterm) == SIG_ERR) {
        return 1;
    }
    if (signal(SIGTERM, myterm) == SIG_ERR) {
        return 1;
    }

    while ((ch = getopt(argc, argv, "hd:s:")) != -1) {
        switch (ch) {
        case 'h':
            usage(argv[0]);
            exit(0);
            break;

        case 'd':
            dest = strdup(optarg);
            break;

        case 's':
            src = strdup(optarg);
            break;

        default:
            break;
        }
    }

    if (src == NULL) {
        TRACE("src required");
        usage(argv[0]);
        exit(1);
    }

    if (dest == NULL) {
        TRACE("dest required");
        usage(argv[0]);
        exit(1);
    }

    argc -= optind;
    argv += optind;

    mrkthr_init();

    mrkthr_spawn("run0", run0, 2, argc, argv);

    mrkthr_loop();

    _shutdown();
    mrkthr_fini();

    return 0;
}
