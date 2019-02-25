AM_MAKEFLAGS = -s

diags = diag.txt
BUILT_SOURCES = diag.c diag.h
EXTRA_DIST = $(diags)
CLEANFILES = $(BUILT_SOURCES) *.core
#CLEANFILES += *.in

lib_LTLIBRARIES = libmrkamqp.la

nobase_include_HEADERS = mrkamqp_private.h mrkamqp.h

# have to move mrkamqp_private.h to nobase_include to expose *_ex() API
#noinst_HEADERS = mrkamqp_private.h

libmrkamqp_la_SOURCES = mrkamqp.c wire.c spec.c frame.c rpc.c
nodist_libmrkamqp_la_SOURCES = diag.c

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

libmrkamqp_la_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 @MRKCOMMON_LOCAL_CFLAGS@ @MRKTHR_LOCAL_CFLAGS@ -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)

libmrkamqp_la_LDFLAGS = -version-info 0:0:0 -L$(libdir) -lmrkthr @MRKTHR_LOCAL_LDFLAGS@ -lmrkcommon -lmndiag @MRKCOMMON_LOCAL_LDFLAGS@

SUBDIRS = .

diag.c diag.h: $(diags)
	$(AM_V_GEN) cat $(diags) | sort -u >diag.txt.tmp && mndiagen -v -S diag.txt.tmp -L mrkamqp -H diag.h -C diag.c *.[ch]

run: all
	for i in $(bin_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;

testrun:
	for i in $(SUBDIRS); do if test "$$i" != "."; then cd $$i && $(MAKE) testrun && cd ..; fi; done;
