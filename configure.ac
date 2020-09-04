dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.68)
AC_INIT([OpenLogReplicator],[0.7.9],[aleszczynski@bersler.com])
AC_CONFIG_HEADERS([config.h])

AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE([foreign])

AC_PROG_CXX
AC_LANG([C++])
AC_FUNC_STRNLEN
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h unistd.h])
AC_FUNC_MALLOC
AC_FUNC_MKTIME
AC_CHECK_FUNCS([memset])
AC_CHECK_HEADER_STDBOOL
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

AC_ARG_WITH([rdkafka],
  [AS_HELP_STRING([--with-rdkafka=PATH], [rdkafka directory])],
  [CPPFLAGS="-I$withval/include $CPPFLAGS -DLINK_LIBRARY_LIBRDKAFKA"; LDFLAGS="-L$withval/lib -lrdkafka++ -lrdkafka $LDFLAGS"]
  [])

AC_ARG_WITH([rapidjson],
  [AS_HELP_STRING([--with-rapidjson=PATH], [rapidjson directory])],
  [CPPFLAGS="-I$withval/include $CPPFLAGS"]
  [])

AC_ARG_WITH([grpc],
  [AS_HELP_STRING([--with-grpc=PATH], [grpc directory])],
  [CPPFLAGS="-I$withval/include -DLINK_LIBRARY_PROTOBUF -DLINK_LIBRARY_GRPC $CPPFLAGS"; LDFLAGS="-L$withval/lib -L$withval/lib64 -lprotobuf -lgrpc++ -lgrpc $LDFLAGS"],
  [])

AC_ARG_WITH([instantclient],
  [AS_HELP_STRING([--with-instantclient=PATH], [instant client directory])],
  [CPPFLAGS="-I$withval/sdk/include -DLINK_LIBRARY_OCI $CPPFLAGS"; LDFLAGS="-L$withval -lclntshcore -lnnz19 -lclntsh $LDFLAGS"],
  [])

CXXFLAGS="$CXXFLAGS -std=c++0x -pedantic -pedantic-errors -w -Wall -Wextra -fmessage-length=0"
LDFLAGS="$LDFLAGS -lpthread"

#AX_PTHREAD

AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT

