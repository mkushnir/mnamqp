AC_INIT(mrkamqp, 0.1, markiyan.kushnir@gmail.com, mrkamqp, https://github.com/mkushnir)
if test -n "$USE_GCC"
then
    probe="gcc${USE_GCC}"
    if which $probe >/dev/null
    then
        CC="gcc${USE_GCC}"
        CXX="c++${USE_GCC}"
        CPP="cpp"
    else
        if which gcc >/dev/null
        then
            CC="gcc"
            CXX="g++"
            CPP="cpp"
        else
            echo "Cannot find gcc"
            exit 1
        fi
    fi
else
    if which clang >/dev/null
    then
        CC="clang -Qunused-arguments -fcolor-diagnostics"
        CXX="clang++ -Qunused-arguments -fcolor-diagnostics"
        CPP="clang-cpp"
        clang_version=`clang --version | grep version | awk '{print $4;}'`
        clang_version_mjr=`echo $clang_version | awk -F. '{print $1;}'`
        clang_version_mnr=`echo $clang_version | awk -F. '{print $2;}'`
        if test "$clang_version_mjr" = "3" -a "$clang_version_mnr" -lt 3
        then
            AC_SUBST(CLANG_DEBUG, ['-ftrapv -fcatch-undefined-behavior'])
        else
            # we assume all others are 3.3+, add here options if needed
            AC_SUBST(CLANG_DEBUG, ['-ftrapv'])
        fi
    else
        CC="cc"
        CXX="c++"
        CPP="cpp"
    fi
fi

AC_CANONICAL_HOST
AC_CANONICAL_TARGET
case "$target_os" in
    *linux*)
        AC_SUBST(_GNU_SOURCE_MACRO, ['-D_GNU_SOURCE'])
    ;;
    *)
        AC_SUBST(_GNU_SOURCE_MACRO)
    ;;
esac

AC_CONFIG_MACRO_DIR([m4])

AC_LANG(C)

AC_PROG_CC
AC_PROG_CXX
AM_PROG_AR
AC_PROG_RANLIB
AC_PROG_INSTALL

LT_INIT
AM_INIT_AUTOMAKE
AM_SILENT_RULES([yes])

dnl AM_MAINTAINER_MODE

AC_CHECK_HEADERS([arpa/inet.h endian.h sys/endian.h])
AC_CHECK_HEADERS([netdb.h])
AC_CHECK_HEADERS([netinet/in.h])
AC_CHECK_HEADERS([sys/socket.h])
AC_CHECK_HEADER_STDBOOL
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_INT8_T
AC_TYPE_UINT8_T
AC_TYPE_INT16_T
AC_TYPE_UINT16_T
AC_TYPE_INT32_T
AC_TYPE_UINT32_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([socket])
AC_CHECK_FUNCS([strdup])



AC_CONFIG_FILES([Makefile src/Makefile test/Makefile man/Makefile])

AC_CONFIG_HEADERS(config.h)

AC_FUNC_MALLOC
AC_FUNC_REALLOC

AC_PRESERVE_HELP_ORDER

AC_SUBST(MRKCOMMON_LOCAL_CFLAGS, [''])
AC_SUBST(MRKCOMMON_LOCAL_LDFLAGS, [''])

AC_SUBST(MRKTHR_LOCAL_CFLAGS, [''])
AC_SUBST(MRKTHR_LOCAL_LDFLAGS, [''])

AC_ARG_ENABLE(debug,
              AC_HELP_STRING([--enable-debug],
                             [Enable debug build (default=no)]),
              [AM_CONDITIONAL([DEBUG], [enable_debug=yes])],
              [AM_CONDITIONAL([DEBUG], [test "$enable_debug" = "yes"])])

AM_CONDITIONAL([LINUX], [echo $build_os | grep linux >/dev/null])
AM_CONDITIONAL([FREEBSD], [echo $build_os | grep freebsd >/dev/null])

#dnl pass LDFLAGS=-L${LOCALBASE}/lib in the environment
AC_CHECK_LIB(mrkcommon, _fini, [], [AC_MSG_FAILURE(libmrkcommon.so is required. Please find it at git@github.com:mkushnir/mrkcommon.git)])

AC_OUTPUT
