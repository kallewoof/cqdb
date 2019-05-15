# Copyright (c) 2013-2018 The Bitcoin Core developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.

ACLOCAL_AMFLAGS = -I build-aux/m4
AM_LDFLAGS = $(LIBTOOL_LDFLAGS) -pthread
EXTRA_LIBRARIES =

CQDB_INCLUDES=-I ./include -I utils/bitcoin

LIBBITCOIN=libbitcoin.a
LIBCQDB=libcqdb.a

EXTRA_LIBRARIES += \
	$(LIBBITCOIN) \
	$(LIBCQDB)

bin_PROGRAMS = test-cqdb
noinst_PROGRAMS =

.PHONY: FORCE check-symbols check-security
# bitcoin core #
BITCOIN_CORE_H = \
		utils/bitcoin/uint256.h \
		utils/bitcoin/utilstrencodings.h

# bitcoin: shared between all the tools
libbitcoin_a_CPPFLAGS = $(AM_CPPFLAGS) $(CQDB_INCLUDES)
libbitcoin_a_CXXFLAGS = $(AM_CXXFLAGS)
libbitcoin_a_SOURCES = \
		utils/bitcoin/uint256.cpp \
		utils/bitcoin/utilstrencodings.cpp \
    $(BITCOIN_CORE_H)

# CQDB #

libcqdb_a_SOURCES = \
	src/cq.cpp \
	src/io.cpp \
	include/cqdb/cq.h \
	include/cqdb/io.h
libcqdb_a_CPPFLAGS = $(AM_CPPFLAGS) $(CQDB_INCLUDES)
libcqdb_a_CXXFLAGS = $(AM_CXXFLAGS) $(PIE_FLAGS)

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
	$(LIBCQDB) \
	$(LIBBITCOIN)

clean-local:
	-rm -f config.h $(LIBBITCOIN)

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
