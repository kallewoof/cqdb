# Copyright (c) 2013-2018 The Bitcoin Core developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.

ACLOCAL_AMFLAGS = -I build-aux/m4
AM_LDFLAGS = $(LIBTOOL_LDFLAGS) -pthread
EXTRA_LIBRARIES =

CQDB_INCLUDES=-I ./include

TESTCQDB=test-cqdb

EXTRA_LIBRARIES += \
	$(LIBCQDB)

TESTS = $(TESTCQDB)
check_PROGRAMS = $(TESTCQDB)
noinst_PROGRAMS = $(TESTCQDB)
lib_LTLIBRARIES = libcqdb.la

.PHONY: FORCE check-symbols check-security

# CQDB #

libcqdb_la_SOURCES = \
	src/cq.cpp \
	src/io.cpp \
	src/uint256.cpp \
	src/utilstrencodings.cpp \
	include/cqdb/cq.h \
	include/cqdb/io.h \
	include/uint256.h \
	include/utilstrencodings.h
libcqdb_la_CPPFLAGS = $(AM_CPPFLAGS) $(CQDB_INCLUDES)
libcqdb_la_CXXFLAGS = $(AM_CXXFLAGS) $(PIE_FLAGS)
cqdbincludedir = $(includedir)/cqdb
cqdbinclude_HEADERS = include/cqdb/cq.h include/cqdb/io.h include/cqdb/uint256.h include/cqdb/utilstrencodings.h

# test-cqdb binary #
test_cqdb_SOURCES = \
	test/catch.hpp \
	test/test-io.cpp \
	test/test-cq.cpp \
	test/test-cqdb.cpp
test_cqdb_CPPFLAGS = $(AM_CPPFLAGS) $(CQDB_INCLUDES)
test_cqdb_CXXFLAGS = $(AM_CXXFLAGS) $(PIE_FLAGS)
test_cqdb_LDFLAGS = $(RELDFLAGS) $(AM_LDFLAGS) $(LIBTOOL_AP_LDFLAGS)

test_cqdb_LDADD = \
	libcqdb.la

clean-local:
	-rm -f config.h

.rc.o:
	@test -f $(WINDRES)
	## FIXME: How to get the appropriate modulename_CPPFLAGS in here?
	$(AM_V_GEN) $(WINDRES) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(CPPFLAGS) -DWINDRES_PREPROC -i $< -o $@

check-symbols: $(bin_PROGRAMS)
if GLIBC_BACK_COMPAT
	@echo "Checking glibc back compat..."
	$(AM_V_at) READELF=$(READELF) CPPFILT=$(CPPFILT) $(top_srcdir)/contrib/devtools/symbol-check.py < $(bin_PROGRAMS)
endif

%.pb.cc %.pb.h: %.proto
	@test -f $(PROTOC)
	$(AM_V_GEN) $(PROTOC) --cpp_out=$(@D) --proto_path=$(<D) $<

uninstall-hook:
	-rmdir ${cqdbincludedir}
