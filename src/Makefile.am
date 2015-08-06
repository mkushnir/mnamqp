AUTOMAKE_OPTIONS = foreign subdir-objects

distdir = $(top_srcdir)/$(PACKAGE)-$(VERSION)/src
dist_HEADERS =

BUILT_SOURCES = diag.c diag.h
EXTRA_DIST = diag.txt gen-diag
CLEANFILES = $(BUILT_SOURCES) *.core
#CLEANFILES += *.in

lib_LTLIBRARIES = libmrkamqp.la

nobase_include_HEADERS = mrkamqp.h

noinst_HEADERS =

libmrkamqp_la_SOURCES = mrkamqp.c
nodist_libmrkamqp_la_SOURCES = diag.c

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

libmrkamqp_la_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(includedir) -I$(top_srcdir) -I$(top_srcdir)/src

libmrkamqp_la_LDFLAGS = -version-info 0:0:0 -L$(libdir) -lmrkcommon

SUBDIRS = .

diag.c diag.h: diag.txt
	sh ./gen-diag mrkamqp

run: all
	for i in $(bin_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;

testrun:
	for i in $(SUBDIRS); do if test "$$i" != "."; then cd $$i && $(MAKE) testrun && cd ..; fi; done;
