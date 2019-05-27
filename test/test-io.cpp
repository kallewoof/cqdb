#include "catch.hpp"

#include <assert.h>

#include <cqdb/io.h>

#include "helpers.h"

TEST_CASE("Basic I/O", "[basic-io]") {
    SECTION("mkrmdir") {
        using cq::mkdir;
        using cq::rmdir;
        using cq::rmdir_r;

        const char* tmpdir = "/tmp/cq.test.io.tmpdir";
        rmdir_r(tmpdir); // we ignore results
        REQUIRE(!rmdir(tmpdir));

        // throw upon attempt to mkdir a place with no permissions
        REQUIRE_THROWS_AS(mkdir("/dev/testdir"), cq::fs_error);
        // return a false value upon trying to make an already existent directory
        REQUIRE(false == mkdir("/tmp"));
        // return a true value upon trying to make a non-existent, permissioned dir
        REQUIRE(true == mkdir(tmpdir));
        REQUIRE(true == rmdir(tmpdir));
        REQUIRE(!rmdir(tmpdir));
    }

    SECTION("rmdir_r") {
        using cq::mkdir;
        using cq::rmdir_r;

        const char* tmpdir = "/tmp/cq.test.io.tmpdir";
        rmdir_r(tmpdir); // ignore results
        REQUIRE(!rmdir_r(tmpdir));

        REQUIRE(true == mkdir(tmpdir));
        std::vector<std::string> files;
        for (int i = 0; i < 3; ++i) {
            std::string filename = std::string(tmpdir) + "/" + std::to_string(i);
            files.push_back(filename);
            FILE* fp = fopen(filename.c_str(), "w");
            fprintf(fp, "hi\n");
            fclose(fp);
        }
        REQUIRE(true == rmdir_r(tmpdir));
        REQUIRE(!rmdir_r(tmpdir));
    }

    SECTION("listdir") {
        using cq::mkdir;
        using cq::rmdir_r;
        using cq::listdir;

        const char* tmpdir = "/tmp/cq.test.io.tmpdir";
        rmdir_r(tmpdir); // ignore results

        REQUIRE(true == mkdir(tmpdir));
        std::vector<std::string> files;
        for (int i = 0; i < 3; ++i) {
            std::string filename = std::string(tmpdir) + "/" + std::to_string(i);
            files.push_back(std::to_string(i));
            FILE* fp = fopen(filename.c_str(), "w");
            fprintf(fp, "hi\n");
            fclose(fp);
        }
        std::vector<std::string> list;
        listdir(tmpdir, list);
        if (list.size() != files.size()) {
            fprintf(stderr, "list with %zu entries (expected %zu):\n", list.size(), files.size());
            for (const auto& s : list) { fprintf(stderr, "- %s\n", s.c_str()); }
            fprintf(stderr, "expected:\n");
            for (const auto& s : files) { fprintf(stderr, "- %s\n", s.c_str()); }
        }
        REQUIRE(list.size() == files.size());
        for (const auto& file : files) {
            REQUIRE(std::find(list.begin(), list.end(), file) != list.end());
        }
        rmdir_r(tmpdir);
    }
}

TEST_CASE("Streams", "[streams") {
    /*
     * The CHV stream (character vector stream) is essential for the test suite, and is tested
     * first.
     */
    SECTION("chv-stream") {
        cq::chv_stream stream;
        uint8_t byte;
        REQUIRE(0 == stream.tell());
        REQUIRE(stream.eof());
        REQUIRE_THROWS(stream.get_uint8());
        stream.seek(1, SEEK_SET);
        REQUIRE(stream.eof());
        REQUIRE(0 == stream.tell());
        byte = 0;
        stream.w(byte);
        byte = 1;
        stream.w(byte);
        REQUIRE(stream.to_string() == "0001");
        stream.seek(-1, SEEK_CUR);
        REQUIRE(1 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(-1, SEEK_CUR);
        REQUIRE(0 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(0, SEEK_END);
        REQUIRE(2 == stream.tell());
        REQUIRE(stream.eof());
        stream.seek(-1, SEEK_END);
        REQUIRE(1 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(-2, SEEK_END);
        REQUIRE(0 == stream.tell());
        REQUIRE(!stream.eof());
        stream.read(&byte, 1);
        REQUIRE(byte == 0);
        REQUIRE(!stream.eof());
        stream.read(&byte, 1);
        REQUIRE(byte == 1);
        REQUIRE(stream.eof());
    }
    SECTION("file-stream") {
        std::string path = "/tmp/cq-io.cpp-test-file-stream";
        // class file : public serializer {
        // private:
        //     long m_tell;
        //     FILE* m_fp;
        // public:
        //     file(FILE* fp);
        //     file(const std::string& path, bool readonly);
        //     ~file() override;
        //     bool eof() override;
        //     size_t write(const uint8_t* data, size_t len) override;
        //     size_t read(uint8_t* data, size_t len) override;
        //     void seek(long offset, int whence) override;
        //     long tell() override;
        // };
        cq::rmfile(path); // ignore return value, we just wanna make sure it doesn't exist already
        cq::file stream(path, false);
        uint8_t byte;
        REQUIRE(0 == stream.tell());
        REQUIRE(stream.eof());
        REQUIRE_THROWS(stream.get_uint8());
        stream.seek(1, SEEK_SET);
        REQUIRE(stream.eof());
        REQUIRE(0 == stream.tell());
        byte = 0;
        stream.w(byte);
        REQUIRE(1 == stream.tell());
        byte = 1;
        stream.w(byte);
        REQUIRE(2 == stream.tell());
        // REQUIRE(stream.to_string() == "0001");
        stream.seek(-1, SEEK_CUR);
        REQUIRE(1 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(-1, SEEK_CUR);
        REQUIRE(0 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(0, SEEK_END);
        REQUIRE(2 == stream.tell());
        REQUIRE(stream.eof());
        stream.seek(-1, SEEK_END);
        REQUIRE(1 == stream.tell());
        REQUIRE(!stream.eof());
        stream.seek(-2, SEEK_END);
        REQUIRE(0 == stream.tell());
        REQUIRE(!stream.eof());
        stream.read(&byte, 1);
        REQUIRE(byte == 0);
        REQUIRE(!stream.eof());
        stream.read(&byte, 1);
        REQUIRE(byte == 1);
        REQUIRE(stream.eof());
    }
    SECTION("Vectors", "[vectors]") {
        std::vector<uint8_t> x{1,2,3};
        cq::chv_stream stream;
        auto pos = stream.tell();
        stream << x;
        auto bytes = stream.tell() - pos;
        REQUIRE(bytes == 4);
        stream.seek(0, SEEK_SET);
        std::vector<uint8_t> y;
        stream >> y;
        REQUIRE(x == y);
    }
}

TEST_CASE("Varints", "[varints]") {
    SECTION("varint") {
        /*
         * pre-defined examples (from bitcoin source):
         * 0:         [0x00]  256:        [0x81 0x00]
         * 1:         [0x01]  16383:      [0xFE 0x7F]
         * 127:       [0x7F]  16384:      [0xFF 0x00]
         * 128:  [0x80 0x00]  16511:      [0xFF 0x7F]
         * 255:  [0x80 0x7F]  65535: [0x82 0xFE 0x7F]
         * 2^32:           [0x8E 0xFE 0xFE 0xFF 0x00]
         */
        uint64_t pde_i[11] =    {0,    1,    127,  128,    255,    4294967296,   256,    16383,  16384,  16511,  65535};
        std::string pde_s[11] = {"00", "01", "7f", "8000", "807f", "8efefeff00", "8100", "fe7f", "ff00", "ff7f", "82fe7f"};
        for (int j = 0; j < 11; ++j) {
            uint64_t i = pde_i[j];
            std::string s = pde_s[j];
            size_t len = s.length() >> 1;
            cq::varint v(i);
            REQUIRE(len == cq::sizer(&v).tell());
            cq::chv_stream stream;
            v.serialize(&stream);
            REQUIRE(stream.to_string() == s);
        }
        // 1 byte: 0b00000000 [0] ~ 0b01111111 [127]
        for (cq::id i = 0; i < 128; ++i) {
            cq::varint v(i);
            REQUIRE(1 == cq::sizer(&v).tell());
            cq::chv_stream stream;
            v.serialize(&stream);
            char buf[15];
            sprintf(buf, "%02llx", i);
            REQUIRE(stream.to_string() == buf);
        }
        // 2 bytes: 0b10000000 00000000 [128] ~ 0b11111111 01111111 [=0b01000000 01111111=0x407f=16511]
        for (cq::id i = 128; i < 16512; ++i) {
            cq::varint v(i);
            REQUIRE(2 == cq::sizer(&v).tell());

            cq::chv_stream stream;
            v.serialize(&stream);
            // we know we can't fit i into 1 byte because it is > 127; we also know it is represented in big endian order,
            // i.e. 0x0011 (for byte 00 and byte 11), except byte 0 has a reserved bit for "has next byte", so:
            // 0b1                  "has another byte"
            //    DCBA987           (i-1) & 1 << {13,12,11,10,9,8,7}
            //           0          "last byte"
            //            6543210   i & 1 << {6,5,4,3,2,1,0}
            #define HIGH_BYTE(i)    (0x80 | ((i >> 7) - 1))
            #define LOW_BYTE(i)     (i & 0x7f)
            char buf[15];
            sprintf(buf, "%02llx%02llx", HIGH_BYTE(i), LOW_BYTE(i));
            #undef HIGH_BYTE
            #undef LOW_BYTE
            REQUIRE(stream.to_string() == buf);
        }
        // 3 bytes
        for (cq::id i = 16513; i < 2113664; ++i) {
            cq::varint v(i);
            REQUIRE(3 == cq::sizer(&v).tell());

            cq::chv_stream stream;
            v.serialize(&stream);
            uint64_t n = i;
            uint8_t byte2 = n & 0x7f;           n = (n >> 7) - 1;
            uint8_t byte1 = 0x80 | (n & 0x7f);  n = (n >> 7) - 1;
            uint8_t byte0 = 0x80 | (n & 0x7f);
            char buf[25];
            sprintf(buf, "%02x%02x%02x", byte0, byte1, byte2);
            REQUIRE(stream.to_string() == buf);
            i += i * 0.000129;
        }
    }

    SECTION("conditional varint") {
        for (uint8_t bits = 1; bits < 8; ++bits) {
            SECTION(std::to_string(bits) + " bit" + (bits == 1 ? "" : "s")) {
                // 0 bytes: [0b00000000 ~ 0b0BIBIBIB], where BIBIBIB occupies <bits> bits (all 1 = "gots more", so cap is actually 1 less)
                cq::id cap = (1 << bits) - 1;
                assert(cap > 0 && cap < 0x80);
                auto s = get_varint(bits, 0);
                {
                    cq::chv_stream stream;
                    for (cq::id i = 0; i < cap; ++i) {
                        s->m_value = i;
                        s->cond_serialize(&stream);
                        // should occupy 0 bytes because the bits contain the entire thing
                        REQUIRE(0 == stream.tell());
                        // deserializing should also take 0 bytes, given an input value
                        s->cond_deserialize(i, &stream);
                        REQUIRE(0 == stream.tell()); // should still be at byte 0
                    }
                }
                // varint part should occupy exactly 1 byte in the region [cap .. cap + 127]
                for (cq::id i = cap; i < cap + 128; ++i) {
                    s->m_value = i;
                    cq::chv_stream stream;
                    s->cond_serialize(&stream);
                    REQUIRE(1 == stream.tell());
                    stream.seek(0, SEEK_SET);
                    s->cond_deserialize(cap, &stream);
                    REQUIRE(i == s->m_value);
                    REQUIRE(1 == stream.tell());
                }
                // varint part should occupy exactly 2 bytes in the region [cap + 128 .. cap + 16511]
                for (cq::id i = cap + 128; i < cap + 16512; ++i) {
                    s->m_value = i;
                    cq::chv_stream stream;
                    s->cond_serialize(&stream);
                    REQUIRE(2 == stream.tell());    
                    stream.seek(0, SEEK_SET);
                    s->cond_deserialize(cap, &stream);
                    REQUIRE(i == s->m_value);
                    REQUIRE(2 == stream.tell());
                }
                delete s;
            }
        }
    }

}

// we store two ordered series (ordered tuples if you will) in the specialized incmap construct
TEST_CASE("Incmaps", "[incmaps]") {
    SECTION("empty") {
        cq::incmap refmap;
        // it should be possible to store the above refmap in 1 byte: size (1)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(1 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        REQUIRE(refmap == refmap2);
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
    }

    SECTION("one small keypair") {
        cq::incmap refmap;
        refmap.m[1] = 2;
        // it should be possible to store the above refmap in 3 bytes: size (1), left value (1), right value (1)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(3 == stream.tell());
        REQUIRE(stream.to_string() == "010102");
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        REQUIRE(1 == refmap2.m.count(1));
        REQUIRE(2 == refmap2.m.at(1));
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("one big key one small value") {
        cq::incmap refmap;
        refmap.m[2113662] = 2;
        // it should be possible to store the above refmap in 5 bytes: size (1), key (3), value (1)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(5 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        REQUIRE(1 == refmap2.m.count(2113662));
        REQUIRE(2 == refmap2.m.at(2113662));
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("one small key one big value") {
        cq::incmap refmap;
        refmap.m[1] = 2113662;
        // it should be possible to store the above refmap in 5 bytes: size (1), key (1), value (3)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(5 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        REQUIRE(1 == refmap2.m.count(1));
        REQUIRE(2113662 == refmap2.m.at(1));
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("one big keypair") {
        cq::incmap refmap;
        refmap.m[2113662] = 2113663;
        // it should be possible to store the above refmap in 7 bytes: size (1), left value (3), right value (3)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(7 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        REQUIRE(1 == refmap2.m.count(2113662));
        REQUIRE(2113663 == refmap2.m.at(2113662));
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("two small keypairs") {
        cq::incmap refmap;
        refmap.m[1] = 2;
        refmap.m[3] = 4;
        // it should be possible to store the above refmap in 5 bytes: size (1), left values (1, 1), right values (1, 1)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(5 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("one small one big keypair") {
        cq::incmap refmap;
        refmap.m[1] = 2;
        refmap.m[2113662] = 2113663;
        // it should be possible to store the above refmap in 9 bytes: size (1), left values (1, 3), right values (1, 3)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(9 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("tightly knit ball at high value") {
        // this tests the concept that series are relative, i.e. the first value here requires 3 bytes to represent, but
        // the rest only require a single byte
        cq::incmap refmap;
        for (cq::id i = 2100000; i < 2100010; ++i) {
            refmap.m[i] = i;
        }
        // it should be possible to store the above refmap in 1 + 2 * 3 + 9 * 2 = 25 bytes
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(25 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("two tightly knit balls at separated high values") {
        // this tests the concept of in-between multi-byte hops
        // *** 2100000 2100001 2100002 ... 2100010 *** 4200010 4200011 ...
        //     2100000 1       1       ... 1           2100000 1       ...
        cq::incmap refmap;
        for (cq::id i = 2100000; i < 2100011; ++i) {
            refmap.m[i] = i;
        }
        for (cq::id i = 4200010; i < 4200021; ++i) {
            refmap.m[i] = i;
        }
        // it should be possible to store the above refmap as:
        // 20                   size (1 byte)
        // 2100000 = 2100000    left/right values offset from 0 (3*2 bytes)
        // 2100001...2100010    left/right values as +1/+1 10 times (10*2 bytes)
        // 4200010 = 4200010    left/right values offset from 2100010 (3*2 bytes)
        // -/-                  10*2 bytes
        // i.e. 1 + 6 + 20 + 6 + 20 = 53 bytes
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(53 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }

    SECTION("multi-byte size") {
        // this tests a size that requires multiple bytes to represent
        cq::incmap refmap;
        for (cq::id i = 0; i < 300; ++i) {
            refmap.m[i] = i;
        }
        // it should be possible to store the above refmap in 602 bytes (size=2, then each keypair as 2 bytes)
        cq::chv_stream stream;
        stream << refmap;
        REQUIRE(602 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::incmap refmap2;
        stream >> refmap2;
        cq::chv_stream stream2;
        stream2 << refmap2;
        REQUIRE(stream.to_string() == stream2.to_string());
        REQUIRE(refmap == refmap2);
    }
}

TEST_CASE("Unordered set", "[unordered_set]") {
    SECTION("empty") {
        cq::unordered_set set;
        // it should be possible to store the above set in 1 byte: size (1)
        cq::chv_stream stream;
        stream << set;
        REQUIRE(1 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::unordered_set set2;
        stream >> set2;
        REQUIRE(set == set2);
        cq::chv_stream stream2;
        stream2 << set2;
        REQUIRE(stream.to_string() == stream2.to_string());
    }

    SECTION("single small entry") {
        cq::unordered_set set;
        set.m.insert(1);
        // it should be possible to store the above set in 2 bytes: size (1), value (1)
        cq::chv_stream stream;
        stream << set;
        REQUIRE(2 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::unordered_set set2;
        stream >> set2;
        REQUIRE(set == set2);
        cq::chv_stream stream2;
        stream2 << set2;
        REQUIRE(stream.to_string() == stream2.to_string());
    }

    SECTION("single big entry") {
        cq::unordered_set set;
        set.m.insert(2113662);
        // it should be possible to store the above set in 4 bytes: size (1), value (3)
        cq::chv_stream stream;
        stream << set;
        REQUIRE(4 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::unordered_set set2;
        stream >> set2;
        REQUIRE(set == set2);
        cq::chv_stream stream2;
        stream2 << set2;
        REQUIRE(stream.to_string() == stream2.to_string());
    }

    SECTION("one big one small entry") {
        cq::unordered_set set;
        set.m.insert(2113662);
        set.m.insert(2113663);
        // it should be possible to store the above set in 5 bytes: size (1), first value (3), second value (1)
        cq::chv_stream stream;
        stream << set;
        REQUIRE(5 == stream.tell());
        stream.seek(0, SEEK_SET);
        // deserializing should produce an identical refmap
        cq::unordered_set set2;
        stream >> set2;
        REQUIRE(set == set2);
        cq::chv_stream stream2;
        stream2 << set2;
        REQUIRE(stream.to_string() == stream2.to_string());
    }

    SECTION("ids array constructor") {
        //           +123, +2113652, +3,      +222
        cq::id ids[4] = {123,  2113775,  2113778, 2114000};
        //           1b,   3b,       1b,      2b
        cq::unordered_set set(ids, 4);
        cq::unordered_set ctl;
        for (int i = 0; i < 4; ++i) ctl.m.insert(ids[i]);
        REQUIRE(set == ctl);
    }

    SECTION("ids set constructor") {
        std::set<cq::id> ids{123,  2113775,  2113778, 2114000};
        cq::unordered_set set(ids);
        cq::unordered_set ctl;
        for (auto& v : ids) ctl.m.insert(v);
        REQUIRE(set == ctl);
    }
}

TEST_CASE("Clusters", "[clusters]") {
    #define start(cd, c) auto ccd = new_cluster(); auto cd = ccd.first; auto c = ccd.second

    #define startd(cd, c) auto c = new_cluster(&cd).second
    #define restartd(cd, c) auto c = open_cluster(&cd).second
    // class cluster : public serializer {
    // public:
    //     id m_cluster{0};
    //     file* m_file;
    //     cluster_delegate* m_delegate;
    //     cluster(cluster_delegate* delegate);
    //     ~cluster() override;
    //     bool eof() override;
    SECTION("construction") {
        bool death_bool = false;
        {
            start(cd, c);
            cd->death_ptr = &death_bool;
            REQUIRE(!death_bool);
            REQUIRE(c->eof());
        }
        REQUIRE(death_bool);
    }

    SECTION("open_for_writing") {
        start(cd, c);
        REQUIRE(cd->cluster_last(false) == cq::nullid);
        REQUIRE(cd->size() == 0);
        REQUIRE(cd->cluster_last(true) == 0);
        REQUIRE(cd->size() == 1);
        REQUIRE((*cd)[0] == 0);
        REQUIRE(c->eof());
    }

    //     void resume_writing(bool clear = false) {
    //     }

    SECTION("resume writing") {
        start(cd, c);
        c->resume_writing();
        REQUIRE(c->m_file != nullptr);
        REQUIRE(c->m_cluster == 0);
        REQUIRE(cd->cluster_last(false) == 0);
        REQUIRE(c->eof());
    }

    //     size_t write(const uint8_t* data, size_t len) override;

    uint32_t u32 = 0x01234567;
    uint64_t u64 = 0x0123456789abcdef;
    const char * string = "0123456789abcdef";

    SECTION("writing") {
        start(cd, c);
        c->resume_writing();
        *c << u32;
        c->w(u64);
        cq::varint(strlen(string)).serialize(c.get());
        c->write((const uint8_t*)string, strlen(string));
        REQUIRE(c->tell() == sizeof(u32) + sizeof(u64) + 1 + strlen(string));
    }

    //     size_t read(uint8_t* data, size_t len) override;

    SECTION("reading") {
        std::shared_ptr<test_cluster_delegate> cd;
        {
            startd(cd, c);
            c->resume_writing();
            *c << u32;
            c->w(u64);
            cq::varint(strlen(string)).serialize(c.get());
            c->write((const uint8_t*)string, strlen(string));
            REQUIRE(c->tell() == sizeof(u32) + sizeof(u64) + 1 + strlen(string));
        }
        {
            uint32_t u32x;
            uint64_t u64x;
            char buf[17];
            restartd(cd, c);
            REQUIRE(cd->cluster_last(false) == 0);
            c->open(0, false);
            *c >> u32x;
            REQUIRE(u32 == u32x);
            c->r(u64x);
            REQUIRE(u64 == u64x);
            auto len = cq::varint::load(c.get());
            REQUIRE(len == strlen(string));
            c->read((uint8_t*)buf, len);
            buf[len] = 0;
            REQUIRE(std::string(buf) == string);
        }
    }

    SECTION("multiple clusters") {
        std::shared_ptr<test_cluster_delegate> cd;
        {
            startd(cd, c);
            c->resume_writing();
            *c << u32;
            c->w(u64);
            *cd += 1;
            c->resume_writing();
            cq::varint(strlen(string)).serialize(c.get());
            c->write((const uint8_t*)string, strlen(string));
            REQUIRE(c->tell() == 1 + strlen(string));
        }
        {
            uint32_t u32x;
            uint64_t u64x;
            char buf[17];
            restartd(cd, c);
            REQUIRE(cd->cluster_last(false) == 1);
            c->open(0, false);
            *c >> u32x;
            REQUIRE(u32 == u32x);
            c->r(u64x);
            REQUIRE(u64 == u64x);
            auto len = cq::varint::load(c.get());
            REQUIRE(len == strlen(string));
            c->read((uint8_t*)buf, len);
            buf[len] = 0;
            REQUIRE(std::string(buf) == string);
        }
    }

    //     void seek(long offset, int whence) override;
    //     long tell() override;
    //     void flush() override { m_file->flush(); }
    // };
}

TEST_CASE("Indexed clusters", "[indexed-clusters]") {
    #undef start
    #undef startd
    #undef restartd

    #define start(cd, c) auto ccd = new_indexed_cluster(); auto cd = ccd.first; auto c = ccd.second

    #define startd(cd, c) auto c = new_indexed_cluster(&cd).second
    #define restartd(cd, c) auto c = open_indexed_cluster(&cd).second

    // indexed cluster is exclusively overrides of cluster, so we use cluster source as basis
    // class cluster : public serializer {
    // public:
    //     id m_cluster{0};
    //     file* m_file;
    //     cluster_delegate* m_delegate;
    //     cluster(cluster_delegate* delegate);
    //     ~cluster() override;
    //     bool eof() override;
    SECTION("construction") {
        bool death_bool = false;
        {
            start(cd, c);
            cd->death_ptr = &death_bool;
            REQUIRE(!death_bool);
            REQUIRE(c->m_ic.eof());
        }
        REQUIRE(death_bool);
    }

    SECTION("open_for_writing") {
        start(cd, c);
        REQUIRE(cd->cluster_last(false) == cq::nullid);
        REQUIRE(cd->size() == 0);
        REQUIRE(cd->cluster_last(true) == 0);
        REQUIRE(cd->size() == 1);
        REQUIRE((*cd)[0] == 0);
        REQUIRE(c->m_ic.eof());
    }

    //     void resume_writing(bool clear = false) {
    //     }

    SECTION("resume writing") {
        start(cd, c);
        c->m_ic.resume_writing();
        REQUIRE(c->m_ic.m_file != nullptr);
        REQUIRE(c->m_ic.m_cluster == 0);
        REQUIRE(cd->cluster_last(false) == 0);
        REQUIRE(c->m_ic.eof());
    }

    //     size_t write(const uint8_t* data, size_t len) override;

    uint32_t u32 = 0x01234567;
    uint64_t u64 = 0x0123456789abcdef;
    const char * string = "0123456789abcdef";

    SECTION("writing") {
        start(cd, c);
        c->m_ic.resume_writing();
        c->m_ic << u32;
        c->m_ic.w(u64);
        cq::varint(strlen(string)).serialize(&c->m_ic);
        c->m_ic.write((const uint8_t*)string, strlen(string));
        REQUIRE(c->m_ic.tell() == sizeof(cq::id) + sizeof(u32) + sizeof(u64) + 1 + strlen(string));
    }

    //     size_t read(uint8_t* data, size_t len) override;

    SECTION("reading") {
        std::shared_ptr<test_indexed_cluster_delegate> cd;
        {
            startd(cd, c);
            c->m_ic.resume_writing();
            c->m_ic << u32;
            c->m_ic.w(u64);
            cq::varint(strlen(string)).serialize(&c->m_ic);
            c->m_ic.write((const uint8_t*)string, strlen(string));
            REQUIRE(c->m_ic.tell() == sizeof(cq::id) + sizeof(u32) + sizeof(u64) + 1 + strlen(string));
        }
        {
            uint32_t u32x;
            uint64_t u64x;
            char buf[17];
            restartd(cd, c);
            REQUIRE(cd->cluster_last(false) == 0);
            c->m_ic.open(0, true);
            c->m_ic >> u32x;
            REQUIRE(u32 == u32x);
            c->m_ic.r(u64x);
            REQUIRE(u64 == u64x);
            auto len = cq::varint::load(&c->m_ic);
            REQUIRE(len == strlen(string));
            c->m_ic.read((uint8_t*)buf, len);
            buf[len] = 0;
            REQUIRE(std::string(buf) == string);
        }
    }

    SECTION("multiple clusters") {
        std::shared_ptr<test_indexed_cluster_delegate> cd;
        {
            startd(cd, c);
            c->m_ic.resume_writing();
            c->m_ic << u32;
            c->m_ic.w(u64);
            *cd += 1;
            c->m_ic.resume_writing();
            cq::varint(strlen(string)).serialize(&c->m_ic);
            c->m_ic.write((const uint8_t*)string, strlen(string));
            REQUIRE(c->m_ic.tell() == sizeof(cq::id) + 1 + strlen(string));
        }
        {
            uint32_t u32x;
            uint64_t u64x;
            char buf[17];
            restartd(cd, c);
            REQUIRE(cd->cluster_last(false) == 1);
            c->m_ic.open(0, true);
            c->m_ic >> u32x;
            REQUIRE(u32 == u32x);
            c->m_ic.r(u64x);
            REQUIRE(u64 == u64x);
            auto len = cq::varint::load(&c->m_ic);
            REQUIRE(len == strlen(string));
            c->m_ic.read((uint8_t*)buf, len);
            buf[len] = 0;
            REQUIRE(std::string(buf) == string);
        }
    }
}
