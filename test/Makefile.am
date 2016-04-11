AUTOMAKE_OPTIONS = foreign subdir-objects
CLEANFILES = *.core
#CLEANFILES += *.in

noinst_PROGRAMS=testfoo testpubsub testrpc

distdir = $(top_srcdir)/$(PACKAGE)-$(VERSION)/test
dist_HEADERS =

BUILT_SOURCES = ../src/diag.c ../src/diag.h

noinst_HEADERS = unittest.h ../src/mrkamqp.h

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

nodist_testfoo_SOURCES = ../src/diag.c
testfoo_SOURCES = testfoo.c
testfoo_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testfoo_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

nodist_testpubsub_SOURCES = ../src/diag.c
testpubsub_SOURCES = testpubsub.c
testpubsub_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testpubsub_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

nodist_testrpc_SOURCES = ../src/diag.c
testrpc_SOURCES = testrpc.c
testrpc_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testrpc_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

../src/diag.c ../src/diag.h: ../src/diag.txt
	$(AM_V_GEN) /bin/sh ../gen-diag mrkamqp

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;
