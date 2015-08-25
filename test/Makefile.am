AUTOMAKE_OPTIONS = foreign subdir-objects
CLEANFILES = *.core
#CLEANFILES += *.in

noinst_PROGRAMS=testfoo testpubsub

distdir = $(top_srcdir)/$(PACKAGE)-$(VERSION)/test
dist_HEADERS = unittest.h

BUILT_SOURCES = ../src/diag.c ../src/diag.h

noinst_HEADERS = ../src/mrkamqp.h

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

nodist_testfoo_SOURCES = ../src/diag.c
testfoo_SOURCES = testfoo.c
testfoo_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(includedir) -I$(top_srcdir) -I$(top_srcdir)/src
testfoo_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

nodist_testpubsub_SOURCES = ../src/diag.c
testpubsub_SOURCES = testpubsub.c
testpubsub_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(includedir) -I$(top_srcdir) -I$(top_srcdir)/src
testpubsub_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

../src/diag.c ../src/diag.h: ../src/diag.txt
	sh ../src/gen-diag mrkamqp

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;
