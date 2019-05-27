#include "catch.hpp"

#include "helpers.h"

#include <memory>
#include <cqdb/cq.h>

TEST_CASE("Time relative", "[timerel]") {
    // #define time_rel_value(cmd) (((cmd) >> 6) & 0x3)
    // inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

    SECTION("time_rel helpers") {
        for (int i = 0; i < 256; ++i) {
            uint8_t u8 = (uint8_t)i;
            // first 6 bits are the command + known flag
            uint8_t cmd = u8 & 0x3f;
            // last 2 bits are the time rel value
            uint8_t tv = u8 >> 6;
            uint8_t u8dup = cmd | cq::time_rel_bits(tv);
            REQUIRE(tv == cq::time_rel_value(u8));
            REQUIRE(u8dup == u8);
        }
    }

    // #define _read_time(t, m_current_time, timerel) \
    //     t = m_current_time + timerel + (timerel > 2 ? varint::load(m_file) : 0)

    SECTION("_read_time macro") {
        using cq::varint;
        cq::chv_stream stream;
        for (long relative_time = 3; relative_time < 128 /* 2 byte varint */; ++relative_time) {
            varint(relative_time - 3).serialize(&stream);
        }
        long m_current_time = 0;
        long expected_time = 0;
        auto m_file = &stream;
        stream.seek(0, SEEK_SET);
        for (long relative_time = 0; relative_time < 128; ++relative_time) {
            uint8_t timerel = relative_time > 3 ? 3 : relative_time;
            expected_time += relative_time;
            _read_time(m_current_time, m_current_time, timerel);
        }
        REQUIRE(expected_time == m_current_time);
    }

    // #define read_cmd_time(u8, cmd, known, timerel, time, current_time) do { \
    //         u8 = m_file->get_uint8(); \
    //         cmd = (u8 & 0x1f); /* 0b0001 1111 */ \
    //         known = 0 != (u8 & 0x20); \
    //         timerel = time_rel_value(u8); \
    //         _read_time(time, current_time, timerel); \
    //     } while(0)

    SECTION("read_cmd_time macro") {
        using cq::varint;
        using cq::time_rel_value;
        cq::chv_stream stream;
        uint8_t u8;
        uint8_t cmd;
        bool known;
        uint8_t timerel;
        long m_current_time = 0;
        long expected_time = 0;
        auto m_file = &stream;
        for (long relative_time = 0; relative_time < 5; ++relative_time) {
            uint8_t timerelx = relative_time > 2 ? 3 : relative_time;
            varint* rtv = relative_time > 2 ? new varint(relative_time - 3) : nullptr;
            for (uint8_t cmd8 = 0; cmd8 <= 0x1f; ++cmd8) {
                for (uint8_t known8 = 0; known8 < 2; ++known8) {
                    expected_time += relative_time;
                    uint8_t u8x = cmd8 | (known8 << 5) | cq::time_rel_bits(relative_time);
                    stream << u8x;
                    if (rtv) rtv->serialize(&stream);
                    stream.seek(0, SEEK_SET);
                    read_cmd_time(u8, cmd, known, timerel, m_current_time, m_current_time);
                    REQUIRE(u8 == u8x);
                    REQUIRE(cmd == cmd8);
                    REQUIRE(known == known8);
                    REQUIRE(timerel == timerelx);
                    REQUIRE(m_current_time == expected_time);
                    stream.clear();
                }
            }
            if (rtv) delete rtv;
        }
    }

    // #define _write_time(rel, m_current_time, write_time) do { \
    //         if (time_rel_value(rel) > 2) { \
    //             uint64_t tfull = uint64_t(write_time - time_rel_value(rel) - m_current_time); \
    //             varint(tfull).serialize(m_file); \
    //             m_current_time = write_time; \
    //         } else { \
    //             m_current_time += time_rel_value(rel);\
    //         }\
    //     } while (0)

    SECTION("_write_time macro") {
        using cq::varint;
        using cq::time_rel_value;
        long m_current_time = 0;
        long running_time = 0;
        long expected_time = 0;
        cq::chv_stream stream;
        auto m_file = &stream;
        for (long relative_time = 0; relative_time < 132; ++relative_time) {
            size_t need_bytes = 0;
            if (relative_time > 2) {
                varint v(relative_time - 3);
                need_bytes = cq::sizer(&v).m_len;
            }
            running_time += relative_time;
            uint8_t u8 = cq::time_rel_bits(relative_time);
            auto start_pos = stream.tell();
            _write_time(u8, m_current_time, running_time);
            auto bytes = stream.tell() - start_pos;
            REQUIRE(bytes == need_bytes);
        }
        m_current_time = 0;
        stream.seek(0, SEEK_SET);
        for (long relative_time = 0; relative_time < 132; ++relative_time) {
            expected_time += relative_time;
            uint8_t timerel = relative_time < 3 ? relative_time : 3;
            _read_time(m_current_time, m_current_time, timerel);
            REQUIRE(m_current_time == expected_time);
        }
        REQUIRE(m_current_time == running_time);
    }
}

TEST_CASE("chronology", "[chronology]") {
    uint256 hash;

    // template<typename T>
    // class chronology : public db {
    // public:
    //     long m_current_time;
    //     std::map<id, std::shared_ptr<T>> m_dictionary;
    //     std::map<uint256, id> m_references;

    //     chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    //     : m_current_time(0)
    //     , db(dbpath, prefix, cluster_size)
    //     {}

    SECTION("construction") {
        std::string dbpath = "/tmp/cq-db-tests";
        cq::rmdir_r(dbpath);
        {
            test_chronology chronology(dbpath, "chronology", 1008);
            chronology.load();
            // should result in a new folder
            REQUIRE(!cq::mkdir(dbpath));
        }
        REQUIRE(cq::rmdir_r(dbpath));
    }

    //     void push_event(long timestamp, uint8_t cmd, std::shared_ptr<T> subject = nullptr, bool refer_only = true) {
    //         bool known = subject.get() && m_references.count(subject->m_hash);
    //         uint8_t header_byte = cmd | (known << 5) | time_rel_bits(timestamp - m_current_time);
    //         *m_file << header_byte;
    //         _write_time(header_byte, m_current_time, timestamp); // this updates m_current_time
    //         if (subject.get()) {
    //             if (known) {
    //                 refer(subject.get());
    //             } else if (refer_only) {
    //                 refer(subject->m_hash);
    //             } else {
    //                 id obid = store(subject.get());
    //                 m_dictionary[obid] = subject;
    //                 m_references[subject->m_hash] = obid;
    //             }
    //         }
    //     }

    //     bool pop_event(uint8_t& cmd, bool& known) {
    //         uint8_t u8, timerel;
    //         try {
    //             read_cmd_time(u8, cmd, known, timerel, m_current_time);
    //         } catch (std::ios_base::failure& f) {
    //             return false;
    //         }
    //         return true;
    //     }

    //     std::shared_ptr<T>& pop_object(std::shared_ptr<T>& object) { load(object.get()); return object; }
    //     id pop_reference()                                         { return derefer(); }
    //     uint256& pop_reference(uint256& hash)                      { return derefer(hash); }

    long ptime;
    SECTION("pushing one no-subject event") {
        long pos;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_nop);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            auto pos1 = chron->m_ic.m_file->tell();
            auto ctime = chron->m_current_time;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(pos1 == chron->m_ic.m_file->tell());
            REQUIRE(ctime == chron->m_current_time);
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            // known is irrelevant here
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two no-subject events") {
        long pos;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_nop);
            chron->push_event(1557974776, cmd_nop);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            // known is irrelevant here
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing one single subject event") {
        long pos;
        uint256 obhash;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            chron->push_event(1557974775, cmd_add, ob);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            // we did not turn off the 'refer only' flag, so this is an unknown reference
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two subject events in a row") {
        long pos;
        uint256 obhash, obhash2;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            auto ob2 = test_object::make_random_unknown();
            obhash = ob->m_hash;
            obhash2 = ob2->m_hash;
            chron->push_event(1557974775, cmd_add, ob);
            chron->push_event(1557974776, cmd_add, ob2);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing the same subject in two events") {
        // because we are refer_only referencing, both pushes should show the object as unknown
        long pos;
        uint256 obhash;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            chron->push_event(1557974775, cmd_add, ob);
            chron->push_event(1557974776, cmd_del, ob);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two subjects in four events") {
        // because we are refer_only referencing, all pushes should show the objects as unknown
        long pos;
        uint256 obhash;
        uint256 obhash2;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            auto ob2 = test_object::make_random_unknown();
            obhash = ob->m_hash;
            obhash2 = ob2->m_hash;
            chron->push_event(1557974775, cmd_add, ob);
            chron->push_event(1557974776, cmd_add, ob2);
            chron->push_event(1557974777, cmd_del, ob2);
            chron->push_event(1557974778, cmd_del, ob);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974777);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974778 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974778);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushed single subjects (refer_only=false) should be known/remembered") {
        long pos;
        uint256 obhash;
        cq::id obid;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            chron->push_event(1557974775, cmd_reg, ob, false);
            obid = ob->m_sid;
            REQUIRE(obid != 0);
            REQUIRE(chron->m_dictionary.count(obid) == 1);
            REQUIRE(*chron->m_dictionary.at(obid) == *ob);
            REQUIRE(chron->m_references.count(obhash) == 1);
            REQUIRE(chron->m_references.at(obhash) == obid);
        }
        {
            auto chron = open_chronology();
            REQUIRE(chron->m_dictionary.count(obid) == 1);
            REQUIRE(chron->m_dictionary.at(obid)->m_hash == obhash);
            REQUIRE(chron->m_references.count(obhash) == 1);
            REQUIRE(chron->m_references.at(obhash) == obid);
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing one single subject event (refer_only=false)") {
        long pos;
        uint256 obhash;
        cq::id obid;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            chron->push_event(1557974775, cmd_reg, ob, false);
            obid = ob->m_sid;
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two subject events (refer_only=false)") {
        long pos;
        uint256 obhash, obhash2;
        cq::id obid, obid2;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            auto ob2 = test_object::make_random_unknown();
            obhash = ob->m_hash;
            obhash2 = ob2->m_hash;
            chron->push_event(1557974775, cmd_reg, ob, false);
            chron->push_event(1557974776, cmd_reg, ob2, false);
            obid = ob->m_sid;
            obid2 = ob2->m_sid;
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash2);
            REQUIRE(ob->m_sid == obid2);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing the same subject in two events (refer_only=false)") {
        long pos;
        uint256 obhash;
        cq::id obid;
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            chron->push_event(1557974775, cmd_reg, ob, false);
            obid = ob->m_sid;
            chron->push_event(1557974776, cmd_del, ob, false);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == obid);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two subjects in four events (refer_only=false)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false);
            chron->push_event(1557974776, cmd_reg, ob2, false);
            chron->push_event(1557974777, cmd_del, ob, false);
            chron->push_event(1557974778, cmd_del, ob2, false);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob->m_hash);
            REQUIRE(obx->m_sid == ob->m_sid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob2->m_hash);
            REQUIRE(obx->m_sid == ob2->m_sid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974777);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob->m_sid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974778 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974778);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob2->m_sid);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("pushing two subject events (refer_only=false (1), true (2))") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false);
            chron->push_event(1557974776, cmd_add, ob2);
            chron->push_event(1557974777, cmd_del, ob);
            chron->push_event(1557974778, cmd_del, ob2);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob->m_hash);
            REQUIRE(obx->m_sid == ob->m_sid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->m_current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == ob2->m_hash);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974777);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob->m_sid);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974778 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->m_current_time == 1557974778);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == ob2->m_hash);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    //     void push_event(long timestamp, uint8_t cmd, const std::set<std::shared_ptr<T>>& subjects) {
    //         push_event(timestamp, cmd);
    //         object* ts[subjects.size()];
    //         size_t i = 0;
    //         for (auto& tp : subjects) {
    //             ts[i++] = tp.get();
    //         }
    //         refer(ts, i);
    //     }

    //     void pop_references(std::set<id>& known, std::set<uint256>& unknown) {
    //         known.clear();
    //         unknown.clear();
    //         derefer(known, unknown);
    //     }

    SECTION("single event with 2 unknown subjects") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            // known is irrelevant
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<uint256> expected_us{ob->m_hash, ob2->m_hash};
            REQUIRE(known_set.size() == 0);
            REQUIRE(unknown_set.size() == 2);
            REQUIRE(unknown_set == expected_us);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 2 known subjects") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_reg, ob2, false); // write full ref to make ob2 known
            chron->push_event(1557974777, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974777);
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<cq::id> expected_ks{ob->m_sid, ob2->m_sid};
            REQUIRE(known_set.size() == 2);
            REQUIRE(unknown_set.size() == 0);
            REQUIRE(known_set == expected_ks);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 1 known 1 unknown subject") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974776);
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<cq::id> expected_ks{ob->m_sid};
            std::set<uint256> expected_us{ob2->m_hash};
            REQUIRE(known_set.size() == 1);
            REQUIRE(unknown_set.size() == 1);
            REQUIRE(known_set == expected_ks);
            REQUIRE(unknown_set == expected_us);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    //     void pop_reference_hashes(std::set<uint256>& mixed) {
    //         std::set<id> known;
    //         pop_references(known, mixed);
    //         for (id i : known) {
    //             mixed.insert(m_dictionary.at(i)->m_hash);
    //         }
    //     }

    SECTION("single event with 2 unknown subjects (ref as hash)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            // known is irrelevant
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 2 known subjects (ref as hash)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_reg, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974777);
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 1 known 1 unknown subject (ref as hash)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974776);
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    //     void push_event(long timestamp, uint8_t cmd, const std::set<uint256>& subject_hashes) {
    //         push_event(timestamp, cmd);
    //         std::set<std::shared_ptr<T>> pool;
    //         object* ts[subject_hashes.size()];
    //         size_t i = 0;
    //         for (auto& hash : subject_hashes) {
    //             if (m_references.count(hash)) {
    //                 // known
    //                 ts[i] = m_dictionary.at(m_references.at(hash)).get();
    //             } else {
    //                 // unknown
    //                 auto ob = std::make_shared<T>(hash);
    //                 pool.insert(ob);
    //                 ts[i] = ob.get();
    //             }
    //             ++i;
    //         }
    //         refer(ts, i);
    //     }

    SECTION("single event with 2 unknown subjects (push hash set)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        std::set<uint256> set{ob->m_hash, ob2->m_hash};
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_mass, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->m_current_time == 1557974775);
            // known is irrelevant
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 2 known subjects (push hash set)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        std::set<uint256> set{ob->m_hash, ob2->m_hash};
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_reg, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_mass, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974777 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974777);
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    SECTION("single event with 1 known 1 unknown subject (push hash set)") {
        long pos;
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        std::set<uint256> set{ob->m_hash, ob2->m_hash};
        {
            auto chron = new_chronology();
            chron->begin_segment(1);
            pos = chron->m_file->tell();
            chron->push_event(1557974775, cmd_reg, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_mass, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->m_current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974775 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->m_current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(chron->peek_time(ptime));
            REQUIRE(1557974776 == ptime);
            REQUIRE(chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->m_current_time == 1557974776);
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(!chron->peek_time(ptime));
            REQUIRE(!chron->pop_event(cmd, known));
        }
    }

    //     virtual void cluster_changed(id old_cluster_id, id new_cluster_id) override {
    //         m_dictionary.clear();
    //         m_references.clear();
    //     }
    // };

    SECTION("cluster changes") {
        // when a cluster changes, the chronology should purge known objects from memory
        auto ob = test_object::make_random_unknown();
        auto chron = new_chronology();
        chron->begin_segment(1);
        chron->push_event(1557974775, cmd_reg, ob, false);
        REQUIRE(chron->m_dictionary.count(ob->m_sid) == 1);
        chron->registry_closing_cluster(1);
        REQUIRE(chron->m_dictionary.count(ob->m_sid) == 0);
    }
}
