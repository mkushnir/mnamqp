AM_MAKEFLAGS = -s

CLEANFILES = *.core
#CLEANFILES += *.in
AM_LIBTOOLFLAGS = --silent

noinst_PROGRAMS = testfoo testpubsub testrpc testspam testham

noinst_HEADERS = unittest.h

diags = ../src/diag.txt
BUILT_SOURCES = diag.c diag.h

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

nodist_testfoo_SOURCES = diag.c
testfoo_SOURCES = testfoo.c
testfoo_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testfoo_LDFLAGS = -L$(libdir) -lmncommon -lmnthr -L$(top_srcdir)/src/.libs -lmnamqp -lmndiag

nodist_testpubsub_SOURCES = diag.c
testpubsub_SOURCES = testpubsub.c
testpubsub_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testpubsub_LDFLAGS = -L$(libdir) -lmncommon -lmnthr -L$(top_srcdir)/src/.libs -lmnamqp -lmndiag

nodist_testrpc_SOURCES = diag.c
testrpc_SOURCES = testrpc.c
testrpc_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testrpc_LDFLAGS = -L$(libdir) -lmncommon -lmnthr -L$(top_srcdir)/src/.libs -lmnamqp -lmndiag

testspam_SOURCES = testspam.c mygauge.c mygauge.h
testspam_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testspam_LDFLAGS = -L$(libdir) -lmncommon -lmnthr -L$(top_srcdir)/src/.libs -lmnamqp -lmndiag

testham_SOURCES = testham.c mygauge.c mygauge.h
testham_CFLAGS = @_GNU_SOURCE_MACRO@ $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(top_srcdir)/src -I$(top_srcdir) -I$(includedir)
testham_LDFLAGS = -L$(libdir) -lmncommon -lmnthr -L$(top_srcdir)/src/.libs -lmnamqp -lmndiag

diag.c diag.h: $(diags)
	$(AM_V_GEN) cat $(diags) | sort -u >diag.txt.tmp && mndiagen -v -S diag.txt.tmp -L mnamqp -H diag.h -C diag.c ../*.[ch] ./*.[ch]

testham-run: testham
	@if test -x $>; \
	    then LD_LIBRARY_PATH=$(libdir) ./$> $${$(>)_ARGS}; \
	    else true; \
	fi

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i $${i}_ARGS; fi; done;

