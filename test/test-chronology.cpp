#include "catch.hpp"

#include "helpers.h"

#include <memory>
#include <cqdb/cq.h>

TEST_CASE("Time relative", "[timerel]") {
    // #define time_rel_value(cmd) (((cmd) >> 6) & 0x3)
    // inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

    SECTION("time_rel macros") {

    }

    // #define _read_time(t, current_time, timerel) \
    //     if (timerel < 3) { \
    //         t = current_time + timerel; \
    //     } else { \
    //         t = current_time + varint::load(m_file); \
    //     }

    // #define read_cmd_time(u8, cmd, known, timerel, time) do { \
    //         u8 = m_file->get_uint8(); \
    //         cmd = (u8 & 0x0f); \
    //         known = 0 != (u8 & 0x20); \
    //         timerel = time_rel_value(u8); \
    //         _read_time(time, time, timerel); \
    //     } while(0)

    // #define _write_time(rel, current_time, write_time) do { \
    //         if (time_rel_value(rel) > 2) { \
    //             uint64_t tfull = uint64_t(write_time - current_time); \
    //             varint(tfull).serialize(m_file); \
    //             current_time = write_time; \
    //         } else { \
    //             current_time += time_rel_value(rel);\
    //         }\
    //         /*sync();*/\
    //     } while (0)
}

TEST_CASE("chronology", "[chronology]") {
    uint256 hash;

    // template<typename T>
    // class chronology : public db {
    // public:
    //     long current_time;
    //     std::map<id, std::shared_ptr<T>> m_dictionary;
    //     std::map<uint256, id> m_references;

    //     chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    //     : current_time(0)
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
    //         uint8_t header_byte = cmd | (known << 5) | time_rel_bits(timestamp - current_time);
    //         *m_file << header_byte;
    //         _write_time(header_byte, current_time, timestamp); // this updates current_time
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
    //             read_cmd_time(u8, cmd, known, timerel, current_time);
    //         } catch (std::ios_base::failure& f) {
    //             return false;
    //         }
    //         return true;
    //     }

    //     std::shared_ptr<T>& pop_object(std::shared_ptr<T>& object) { load(object.get()); return object; }
    //     id pop_reference()                                         { return derefer(); }
    //     uint256& pop_reference(uint256& hash)                      { return derefer(hash); }

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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->current_time == 1557974775);
            // known is irrelevant here
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_nop == cmd);
            REQUIRE(chron->current_time == 1557974776);
            // known is irrelevant here
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            // we did not turn off the 'refer only' flag, so this is an unknown reference
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974777);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974778);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == obhash);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash2);
            REQUIRE(ob->m_sid == obid2);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == obid);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob->m_hash);
            REQUIRE(obx->m_sid == ob->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob2->m_hash);
            REQUIRE(obx->m_sid == ob2->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974777);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974778);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob2->m_sid);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob->m_hash);
            REQUIRE(obx->m_sid == ob->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974776);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == ob2->m_hash);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974777);
            REQUIRE(known);
            REQUIRE(chron->pop_reference() == ob->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
            REQUIRE(chron->current_time == 1557974778);
            REQUIRE(!known);
            REQUIRE(chron->pop_reference(hash) == ob2->m_hash);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->current_time == 1557974775);
            // known is irrelevant
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<uint256> expected_us{ob->m_hash, ob2->m_hash};
            REQUIRE(known_set.size() == 0);
            REQUIRE(unknown_set.size() == 2);
            REQUIRE(unknown_set == expected_us);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->push_event(1557974776, cmd_reg, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_mass, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974777);
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<cq::id> expected_ks{ob->m_sid, ob2->m_sid};
            REQUIRE(known_set.size() == 2);
            REQUIRE(unknown_set.size() == 0);
            REQUIRE(known_set == expected_ks);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974776);
            std::set<cq::id> known_set;
            std::set<uint256> unknown_set;
            chron->pop_references(known_set, unknown_set);
            std::set<cq::id> expected_ks{ob->m_sid};
            std::set<uint256> expected_us{ob2->m_hash};
            REQUIRE(known_set.size() == 1);
            REQUIRE(unknown_set.size() == 1);
            REQUIRE(known_set == expected_ks);
            REQUIRE(unknown_set == expected_us);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->current_time == 1557974775);
            // known is irrelevant
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974777);
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974776);
            std::set<uint256> set;
            chron->pop_reference_hashes(set);
            std::set<uint256> expected{ob->m_hash, ob2->m_hash};
            REQUIRE(set.size() == 2);
            REQUIRE(set == expected);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            REQUIRE(chron->current_time == 1557974775);
            // known is irrelevant
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974777);
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(false == chron->pop_event(cmd, known));
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
            chron->current_time = 0;
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_reg == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_mass == cmd);
            // known is irrelevant
            REQUIRE(chron->current_time == 1557974776);
            std::set<uint256> set2;
            chron->pop_reference_hashes(set2);
            REQUIRE(set2.size() == 2);
            REQUIRE(set2 == set);
            REQUIRE(false == chron->pop_event(cmd, known));
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
        chron->cluster_changed(1, 30000);
        REQUIRE(chron->m_dictionary.count(ob->m_sid) == 0);
    }
}
