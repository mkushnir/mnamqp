#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h> /* open(2) */

#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>
#include <mrkcommon/traversedir.h>

#include <mrkthr.h>

#include <mrkamqp_private.h>


#include "diag.h"

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif


static char *user = "guest";
static char *password = "guest";
static char *vhost = "/";
static char *src = NULL;
static char *dest = NULL;
//static char *dir = NULL;

static amqp_conn_t *conn = NULL;

static int shutting_down = 0;

#ifndef SIGINFO
UNUSED
#endif
static void
myinfo(UNUSED int sig)
{
    mrkthr_dump_all_ctxes();
}


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
        TRACE("Shutting down. Another signal will cause immediate exit.");
        shutting_down = 1;
        amqp_conn_close(conn);
        amqp_conn_destroy(&conn);
        _shutdown();
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
my_content_cb(UNUSED amqp_frame_t *method,
              UNUSED amqp_frame_t *header,
              char *data,
              UNUSED void *udata)
{
    //TRACE("---");
    //D8(data, header->payload.header->body_size);
    //TRACE("---");
    //CTRACE("C: %s", data);
    //CTRACE("C: %ld", header->payload.header->body_size);
    if (data != NULL) {
        free(data);
    }
    TRACEC("<");
    return 0;
}


static void
my_header_completion_cb(UNUSED amqp_channel_t *chan,
                        amqp_header_t *header,
                        UNUSED void *udata)
{
    header->delivery_mode = 2;
}

static int
traversedir_cb(const char *dir, struct dirent *de, void *udata)
{
    int res;
    amqp_channel_t *chan;
    char *path;

    res = 0;
    chan = udata;

    if (de != NULL) {
        struct stat sb;
        int fd;
        char *buf;

        path = path_join(dir, de->d_name);
        if (stat(path, &sb) != 0) {
            perror("stat");
            goto end;
        }
        //CTRACE("P: %s (%ld)", path, sb.st_size);
        TRACEC(">");

        if ((fd = open(path, O_RDONLY)) == -1) {
            perror("open");
            goto end;
        }

        if ((buf = malloc(sb.st_size)) == NULL) {
            FAIL("malloc");
        }

        if (read(fd, buf, sb.st_size) == -1) {
            perror("read");
            goto end;
        }
        res = amqp_channel_publish(chan,
                                 "",
                                 dest,
                                 0,
                                 my_header_completion_cb,
                                 NULL,
                                 buf,
                                 sb.st_size);
        free(path);
        free(buf);
    }
end:
    return res;
}


static void
mypub1(amqp_channel_t *chan, char *path)
{
    struct stat sb;

    CTRACE("path=%s", path);
    if (stat(path, &sb) != 0) {
        perror("stat");
        return;
    }

    if (S_ISDIR(sb.st_mode)) {
        if (traverse_dir(path, traversedir_cb, chan) != 0) {
            CTRACE("traverse_dir error");
        }
    } else {
        CTRACE("file=%s", path);
    }

}


static int
mypub(UNUSED int argc, void **argv)
{
    amqp_channel_t *chan;
    int _argc;
    char **_argv;
    int i;

    assert(argc == 3);
    chan = argv[0];
    _argc = (intptr_t)argv[1];
    _argv = argv[2];

    mrkthr_sleep(10000);
    CTRACE("Starting publishing ...");

    for (i = 0; i < _argc; ++i) {
        mypub1(chan, _argv[i]);
    }
    CTRACE("Exiting mypub...");
    return 0;
}


static int
create_conn(void)
{
    int res;

    res = 0;

    conn = amqp_conn_new("localhost", 5672, user, password, vhost, 0, 0, 0, AMQP_CAP_PUBLISHER_CONFIRMS);

    if (amqp_conn_open(conn) != 0) {
        res = 1;
        goto err;
    }

    if (amqp_conn_run(conn) != 0) {
        res = 1;
        goto err;
    }

end:
    return res;

err:
    TR(res);
    amqp_conn_close(conn);
    amqp_conn_destroy(&conn);
    goto end;
}


static void
declare_queue_cb1(UNUSED amqp_channel_t *chan,
                  amqp_frame_t *fr,
                  UNUSED void *udata)
{
    amqp_queue_declare_t *m;

    m = (amqp_queue_declare_t *)fr->payload.params;
    table_add_i32(&m->arguments, "x-expires", 3600000);
    table_add_lstr(&m->arguments, "x-ha-policy", bytes_new_from_str("all"));
}


static int
run_conn(int argc, char **argv)
{
    int res;
    amqp_channel_t *chan;
    amqp_consumer_t *cons;

    res = 0;
    assert(conn != NULL);

    if ((chan = amqp_create_channel(conn)) == NULL) {
        res = 1;
        goto err;
    }

    //if (amqp_channel_confirm(chan, 0) != 0) {
    //    res = 1;
    //    goto err;
    //}

    if (amqp_channel_declare_queue_ex(chan,
                                      src,
                                      DECLARE_QUEUE_FEXCLUSIVE,
                                      declare_queue_cb1,
                                      NULL,
                                      NULL) != 0) {
        res = 1;
        goto err;
    }

    if ((cons = amqp_channel_create_consumer(chan,
                                             src,
                                             NULL,
                                             CONSUME_FNOACK)) == NULL) {
        res = 1;
        goto err;
    }

    mrkthr_spawn("pub", mypub, 3, chan, argc, argv);

    (void)amqp_consumer_handle_content(cons, my_content_cb, NULL);

    if (amqp_close_consumer(cons) != 0) {
        res = 1;
        goto err;
    }

    if (amqp_close_channel(chan) != 0) {
        res = 1;
        goto err;
    }

end:
    if (conn != NULL) {
        (void)amqp_conn_close(conn);
    }
    amqp_conn_destroy(&conn);
    return res;

err:
    TR(res);
    goto end;
}

static int
run0(UNUSED int argc, void **argv)
{
    int res;
    int _argc;
    char **_argv;

    assert(argc == 2);

    res = 0;

    _argc = (intptr_t)argv[0];
    _argv = argv[1];

    mrkamqp_init();

    while (!shutting_down) {
        if (create_conn() != 0) {
            goto err;
        }
        if (run_conn(_argc, _argv) != 0) {
            goto err;
        }

err:
        assert(conn == NULL);
        CTRACE("Reconnecting ...");
        mrkthr_sleep(1000);
        continue;
    }

    CTRACE("Exiting run0 ...");
    return res;
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
#ifdef SIGINFO
    if (signal(SIGINFO, myinfo) == SIG_ERR) {
        return 1;
    }
#endif

    while ((ch = getopt(argc, argv, "d:hs:")) != -1) {
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
