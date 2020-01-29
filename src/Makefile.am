AM_MAKEFLAGS = -s

diags = diag.txt
BUILT_SOURCES = diag.c diag.h
EXTRA_DIST = $(diags)
CLEANFILES = $(BUILT_SOURCES) *.core
#CLEANFILES += *.in
AM_LIBTOOLFLAGS = --silent

lib_LTLIBRARIES = libmnamqp.la

nobase_include_HEADERS = mnamqp_private.h mnamqp.h

# have to move mnamqp_private.h to nobase_include to expose *_ex() API
#noinst_HEADERS = mnamqp_private.h

libmnamqp_la_SOURCES = mnamqp.c wire.c spec.c frame.c rpc.c
nodist_libmnamqp_la_SOURCES = diag.c

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

libmnamqp_la_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 @MNCOMMON_LOCAL_CFLAGS@ @MNTHR_LOCAL_CFLAGS@ -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)

libmnamqp_la_LDFLAGS = -version-info 0:0:0 -L$(libdir) -lmnthr @MNTHR_LOCAL_LDFLAGS@ -lmncommon -lmndiag @MNCOMMON_LOCAL_LDFLAGS@

SUBDIRS = .

diag.c diag.h: $(diags)
	$(AM_V_GEN) cat $(diags) | sort -u >diag.txt.tmp && mndiagen -v -S diag.txt.tmp -L mnamqp -H diag.h -C diag.c *.[ch]

run: all
	for i in $(bin_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;

testrun:
	for i in $(SUBDIRS); do if test "$$i" != "."; then cd $$i && $(MAKE) testrun && cd ..; fi; done;
