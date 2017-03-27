AUTOMAKE_OPTIONS = foreign subdir-objects
AM_MAKEFLAGS = -s

CLEANFILES = *.core
#CLEANFILES += *.in

noinst_PROGRAMS = testfoo testpubsub testrpc testspam testham

noinst_HEADERS = unittest.h

BUILT_SOURCES = ../src/diag.c ../src/diag.h

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

testspam_SOURCES = testspam.c mygauge.c mygauge.h
testspam_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testspam_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

testham_SOURCES = testham.c mygauge.c mygauge.h
testham_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testham_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkthr -L$(top_srcdir)/src/.libs -lmrkamqp

../src/diag.c ../src/diag.h: ../src/diag.txt
	$(AM_V_GEN) cat ../src/diag.txt | sort -u | /bin/sh ./gen-diag mrkamqp ../src

testham-run: testham
	@if test -x $>; \
	    then LD_LIBRARY_PATH=$(libdir) ./$> $${$(>)_ARGS}; \
	    else true; \
	fi

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i $${i}_ARGS; fi; done;

