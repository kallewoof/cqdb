#include "catch.hpp"

#include "helpers.h"

#include <memory>
#include <cqdb/cq.h>

TEST_CASE("Objects", "[objects]") {
    SECTION("construction") {
        // nothing uses the non-explicit two-param constructor
        test_object empty;
        REQUIRE(empty.m_sid == 0);
        REQUIRE(empty.m_hash == uint256());

        uint256 v = uint256S("0102030405060708090a0b0c0d0e0f1011121314151617181920212223242526");
        test_object with_hash(0, v);
        REQUIRE(with_hash.m_sid == 0);
        REQUIRE(with_hash.m_hash == v);

        test_object with_sid(123); // this also uses non-explicit two-param constructor
        REQUIRE(with_sid.m_sid == 123);
        REQUIRE(with_sid.m_hash == uint256());
    
        test_object with_both(123, v);
        REQUIRE(with_both.m_sid == 123);
        REQUIRE(with_both.m_hash == v);
    }
}

TEST_CASE("Header", "[header]") {
    SECTION("construction") {
        cq::header hdr(255, 1557791681, 0);
        REQUIRE(0 == hdr.get_segment_count());
        REQUIRE(255 == hdr.get_version());
        REQUIRE(1557791681 == hdr.get_timestamp_start());
    }

    SECTION("empty") {
        cq::header hdr(255, 1557791681, 0);
        cq::chv_stream stm;
        stm << hdr;
        stm.seek(0, SEEK_SET);
        cq::header hdr2(0, &stm);
        REQUIRE(0 == hdr2.get_segment_count());
        REQUIRE(255 == hdr2.get_version());
        REQUIRE(1557791681 == hdr2.get_timestamp_start());
    }

    SECTION("single segment") {
        cq::header hdr(255, 1557791681, 0);
        hdr.mark_segment(1, 2);
        REQUIRE(1 == hdr.get_segment_count());
        REQUIRE(2 == hdr.get_segment_position(1));
        cq::chv_stream stm;
        stm << hdr;
        stm.seek(0, SEEK_SET);
        cq::header hdr2(0, &stm);
        REQUIRE(1 == hdr2.get_segment_count());
        REQUIRE(2 == hdr2.get_segment_position(1));
    }

    SECTION("two segments") {
        cq::header hdr(255, 1557791681, 0);
        hdr.mark_segment(1, 2);
        hdr.mark_segment(999999, 3);
        REQUIRE(2 == hdr.get_segment_count());
        REQUIRE(2 == hdr.get_segment_position(1));
        REQUIRE(3 == hdr.get_segment_position(999999));
        cq::chv_stream stm;
        stm << hdr;
        stm.seek(0, SEEK_SET);
        cq::header hdr2(0, &stm);
        REQUIRE(2 == hdr2.get_segment_count());
        REQUIRE(2 == hdr2.get_segment_position(1));
        REQUIRE(3 == hdr2.get_segment_position(999999));
    }
}

TEST_CASE("Registry", "[registry]") {
    test_registry_delegate regdel;
    SECTION("empty") {
        cq::registry empty(&regdel, "/tmp/cq-reg", "reg", 2016);
        REQUIRE(empty.get_clusters().size() == 0);
        cq::chv_stream stream;
        stream << empty;
        // should be able to serialize above registry in 6 bytes (size (1) + cluster size (4) + tip (1))
        REQUIRE(stream.tell() == 6);
        cq::registry reg2(&regdel, "/tmp/cq-reg", "reg");
        stream.seek(0, SEEK_SET);
        stream >> reg2;
        REQUIRE(empty == reg2);
    }

    SECTION("one entry") {
        cq::registry one(&regdel, "/tmp/cq-reg", "reg", 2016);
        one.prepare_cluster_for_segment(1 * 2016);
        REQUIRE(one.get_clusters().size() == 1);
        cq::chv_stream stream;
        stream << one;
        // should be able to serialize above registry in 7 bytes (size (1) + entry (1) + cluster size (4) + tip (1))
        REQUIRE(stream.tell() == 7);
        cq::registry reg2(&regdel, "/tmp/cq-reg", "reg");
        stream.seek(0, SEEK_SET);
        stream >> reg2;
        REQUIRE(one == reg2);
    }

    SECTION("two entries") {
        cq::registry reg(&regdel, "/tmp/cq-reg", "reg", 2016);
        reg.prepare_cluster_for_segment(1 * 2016);
        reg.prepare_cluster_for_segment(128 * 2016);
        REQUIRE(128 == reg.cluster_next(1));
        REQUIRE(reg.get_clusters().size() == 2);
        cq::chv_stream stream;
        stream << reg;
        // should be able to serialize above registry in 8 bytes (size (1) + entries (1 + 1) + cluster size (4) + tip (1))
        // note that entry 2 is 128, but relative, so 127
        REQUIRE(stream.tell() == 8);
        cq::registry reg2(&regdel, "/tmp/cq-reg", "reg");
        stream.seek(0, SEEK_SET);
        stream >> reg2;
        REQUIRE(reg == reg2);
    }

    SECTION("opening clusters for segments") {
        cq::registry reg(&regdel, "/tmp/cq-reg", "reg", 2016);
        REQUIRE(reg.get_clusters().size() == 0);
        REQUIRE(reg.prepare_cluster_for_segment(2015) == 0);
        REQUIRE(reg.get_clusters().size() == 1);
        REQUIRE(reg.prepare_cluster_for_segment(2016) == 1);
        REQUIRE(reg.get_clusters().size() == 2);
        cq::chv_stream stream;
        stream << reg;
        // should be able to serialize above registry in 8 bytes (size (1) + entries (1 + 1) + cluster size (4) + tip (1))
        // why 1 + 1 despite 2015? because we are storing the *cluster numbers* not the segment ids, i.e. 0 and 1 not 2015 and 2016
        // why 1 for tip, despite it being 2016? because tip is serialized as (tip - cluster * cluster_size) i.e. (2016 - 1 * 2016) = 0, here
        REQUIRE(stream.tell() == 8);
        cq::registry reg2(&regdel, "/tmp/cq-reg", "reg");
        stream.seek(0, SEEK_SET);
        stream >> reg2;
        REQUIRE(reg == reg2);
    }
}

TEST_CASE("Database", "[db]") {
    // class db {
    // protected:
    //     const std::string m_dbpath;
    //     const std::string m_prefix;
    //     id m_cluster;
    //     registry m_reg;
    //     header* m_header;
    //     bool m_readonly;

    //     serializer* open(const std::string& fname, bool readonly);
    //     serializer* open(id fref, bool readonly);

    //     void close();

    // public:
    //     serializer* m_file;

    //     // struction
    //     db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024);
    //     ~db();

    SECTION("construction") {
        std::string dbpath = "/tmp/cq-db-tests";
        cq::rmdir_r(dbpath);
        {
            cq::db db(dbpath, "cluster", 1008);
            db.load();
            // should result in a new folder
            REQUIRE(!cq::mkdir(dbpath));
        }
        REQUIRE(cq::rmdir_r(dbpath));
    }

    SECTION("beginning segments") {
        auto db = new_db();
        const cq::registry& reg = db->get_registry();
        REQUIRE(0 == reg.m_tip);
        db->begin_segment(1);
        REQUIRE(1 == reg.m_tip);
        db->begin_segment(2);
        REQUIRE(2 == reg.m_tip);
        // should throw db_error if attempting to begin an earlier segment
        REQUIRE_THROWS_AS(db->begin_segment(1), cq::db_error);
        // should not have changed registry
        REQUIRE(2 == reg.m_tip);
    }

    //     // registry
    //     id store(object* t);                    // writes object to disk and returns its absolute id

    SECTION("storing a single object") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        db->begin_segment(1);
        auto obid = db->store(ob.get());
        REQUIRE(obid > 0);
        REQUIRE(obid == ob->m_sid);
    }

    SECTION("storing two objects") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto obid2 = db->store(ob2.get());
        REQUIRE(obid > 0);
        REQUIRE(obid == ob->m_sid);
        REQUIRE(obid2 > 0);
        REQUIRE(obid2 == ob2->m_sid);
        REQUIRE(obid != obid2);
    }

    SECTION("storing the same object twice") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        REQUIRE(obid > 0);
        REQUIRE(obid == ob->m_sid);
        auto obid2 = db->store(ob.get());
        REQUIRE(obid2 > 0);
        REQUIRE(obid2 == ob->m_sid);
        REQUIRE(obid != obid2);
    }

    //     void load(object* t);                   // reads object from disk at current position
    //     void fetch(object* t, id i);            // fetches object with obid 'i' into *t from disk

    SECTION("storing then fetching a single object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        test_object ob2;
        db->fetch(&ob2, obid);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
    }

    SECTION("should remember file states on reopen") {
        cq::id obid;
        uint256 obhash;
        long pos;
        {
            auto db = new_db();
            auto ob = test_object::make_random_unknown();
            obhash = ob->m_hash;
            db->begin_segment(1);
            pos = db->m_file->tell();
            obid = db->store(ob.get());
        }
        {
            auto db = open_db();
            db->m_file->seek(pos, SEEK_SET);
            test_object ob;
            db->load(&ob);
            REQUIRE(ob.m_sid == obid);
            REQUIRE(ob.m_hash == obhash);
        }
    }

    SECTION("storing then loading a single object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        auto obid = db->store(ob.get());
        test_object ob2;
        db->m_file->seek(pos, SEEK_SET);
        db->load(&ob2);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
    }

    SECTION("storing one then attempting to load two objects") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        auto obid = db->store(ob.get());
        test_object ob2;
        db->m_file->seek(pos, SEEK_SET);
        db->load(&ob2);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
        REQUIRE_THROWS_AS(db->load(&ob2), cq::io_error);
    }

    SECTION("storing two objects in a row") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto obid2 = db->store(ob2.get());
        test_object ob3;
        test_object ob4;
        // load 2nd, then 1st
        db->fetch(&ob3, obid2);
        REQUIRE(ob2->m_hash == ob3.m_hash);
        REQUIRE(ob2->m_sid == ob3.m_sid);
        REQUIRE(*ob2 == ob3);
        db->fetch(&ob4, obid);
        REQUIRE(ob->m_hash == ob4.m_hash);
        REQUIRE(ob->m_sid == ob4.m_sid);
        REQUIRE(*ob == ob4);
    }

    SECTION("storing two objects, with a segment in between") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        db->begin_segment(1);
        auto obid = db->store(ob.get());
        db->begin_segment(2);
        auto obid2 = db->store(ob2.get());
        test_object ob3;
        test_object ob4;
        // load 2nd, then 1st
        db->fetch(&ob3, obid2);
        REQUIRE(ob2->m_hash == ob3.m_hash);
        REQUIRE(ob2->m_sid == ob3.m_sid);
        REQUIRE(*ob2 == ob3);
        db->fetch(&ob4, obid);
        REQUIRE(ob->m_hash == ob4.m_hash);
        REQUIRE(ob->m_sid == ob4.m_sid);
        REQUIRE(*ob == ob4);
    }

    //     void refer(object* t);                  // writes a reference to t to disk

    SECTION("writing reference to known object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        db->refer(ob.get());
        test_object ob2;
        db->fetch(&ob2, obid);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
    }

    //     id derefer();                           // reads a reference id from disk

    SECTION("reading reference to known object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto pos = db->m_file->tell();
        db->refer(ob.get());
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer() == obid);
    }

    SECTION("reading reference to unknown object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        db->refer(ob->m_hash);
        db->m_file->seek(pos, SEEK_SET);
        uint256 h;
        REQUIRE(db->derefer(h) == ob->m_hash);
    }

    SECTION("reading reference to two known objects") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto obid2 = db->store(ob2.get());
        auto pos = db->m_file->tell();
        db->refer(ob2.get());
        db->refer(ob.get());
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer() == obid2);
        REQUIRE(db->derefer() == obid);
    }

    SECTION("reading reference to two unknown objects") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        db->refer(ob2->m_hash);
        db->refer(ob->m_hash);
        db->m_file->seek(pos, SEEK_SET);
        uint256 h;
        REQUIRE(db->derefer(h) == ob2->m_hash);
        REQUIRE(db->derefer(h) == ob->m_hash);
    }

    SECTION("reading reference to one known one unknown object") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto pos = db->m_file->tell();
        db->refer(ob2->m_hash);
        db->refer(ob.get());
        db->m_file->seek(pos, SEEK_SET);
        uint256 h;
        REQUIRE(db->derefer(h) == ob2->m_hash);
        REQUIRE(db->derefer() == obid);
    }

    SECTION("ob, ref, ob2, ref2 [2 known]") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto pos = db->m_file->tell();
        db->refer(ob.get());
        auto obid2 = db->store(ob2.get());
        auto pos2 = db->m_file->tell();
        db->refer(ob2.get());
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer() == obid);
        db->m_file->seek(pos2, SEEK_SET);
        REQUIRE(db->derefer() == obid2);
    }

    SECTION("ob, ref, ob2, ref2 [L known]") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto obid = db->store(ob.get());
        auto pos = db->m_file->tell();
        db->refer(ob.get());
        auto pos2 = db->m_file->tell();
        db->refer(ob2->m_hash);
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer() == obid);
        uint256 h;
        db->m_file->seek(pos2, SEEK_SET);
        REQUIRE(db->derefer(h) == ob2->m_hash);
    }

    SECTION("ob, ref, ob2, ref2 [R known]") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        db->refer(ob->m_hash);
        auto obid2 = db->store(ob2.get());
        auto pos2 = db->m_file->tell();
        db->refer(ob2.get());
        uint256 h;
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer(h) == ob->m_hash);
        db->m_file->seek(pos2, SEEK_SET);
        REQUIRE(db->derefer() == obid2);
    }

    SECTION("ob, ref, ob2, ref2 [2 unknown]") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto pos = db->m_file->tell();
        db->refer(ob->m_hash);
        auto pos2 = db->m_file->tell();
        db->refer(ob2->m_hash);
        uint256 h;
        db->m_file->seek(pos, SEEK_SET);
        REQUIRE(db->derefer(h) == ob->m_hash);
        db->m_file->seek(pos2, SEEK_SET);
        REQUIRE(db->derefer(h) == ob2->m_hash);
    }

    //     void refer(object** ts, size_t sz);     // writes an unordered set of references to sz number of objects
    //     void derefer(std::set<id>& known        // reads an unordered set of references from disk
    //                , std::set<uint256> unknown);

    SECTION("unordered set of 1 known 0 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        cq::object* ts[] = {ob.get()};

        auto obid = db->store(ob.get());

        auto pos = db->m_file->tell();
        db->refer(ts, 1);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 1);
        REQUIRE(unknown.size() == 0);
        std::vector<cq::id> knownv;
        for (const auto& k : known) knownv.push_back(k);
        REQUIRE(knownv[0] == obid);
    }

    SECTION("unordered set of 0 known 1 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        cq::object* ts[] = {ob.get()};

        auto pos = db->m_file->tell();
        db->refer(ts, 1);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 0);
        REQUIRE(unknown.size() == 1);
        std::vector<uint256> unknownv;
        for (const auto& u : unknown) unknownv.push_back(u);
        REQUIRE(unknownv[0] == ob->m_hash);
    }

    SECTION("unordered set of 2 known 0 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        cq::object* ts[] = {ob.get(), ob2.get()};

        auto obid = db->store(ob.get());
        auto obid2 = db->store(ob2.get());

        auto pos = db->m_file->tell();
        db->refer(ts, 2);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 2);
        REQUIRE(unknown.size() == 0);
        std::vector<cq::id> knownv;
        for (const auto& k : known) knownv.push_back(k);
        REQUIRE(knownv[0] == obid);
        REQUIRE(knownv[1] == obid2);
    }

    SECTION("unordered set of 0 known 2 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        cq::object* ts[] = {ob.get(), ob2.get()};

        auto pos = db->m_file->tell();
        db->refer(ts, 2);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 0);
        REQUIRE(unknown.size() == 2);
        // we need to sort our hashes, because the results above are in sorted order, not chrono
        std::set<uint256> expected;
        expected.insert(ob->m_hash);
        expected.insert(ob2->m_hash);
        REQUIRE(expected == unknown);
    }

    SECTION("unordered set of 1 known 1 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        cq::object* ts[] = {ob.get(), ob2.get()};

        auto obid = db->store(ob.get());

        auto pos = db->m_file->tell();
        db->refer(ts, 2);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 1);
        REQUIRE(unknown.size() == 1);
        REQUIRE(*known.begin() == obid);
        REQUIRE(*unknown.begin() == ob2->m_hash);
    }

    SECTION("unordered set of 20 known 0 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        cq::object* ts[20];
        std::set<cq::id> known_set;
        std::set<std::shared_ptr<test_object>> known_refs;
        for (int i = 0; i < 20; ++i) {
            auto ob = test_object::make_random_unknown();
            auto obid = db->store(ob.get());
            known_refs.insert(ob);
            known_set.insert(obid);
            ts[i] = ob.get();
        }

        auto pos = db->m_file->tell();
        db->refer(ts, 20);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 20);
        REQUIRE(unknown.size() == 0);
        REQUIRE(known == known_set);
    }

    SECTION("unordered set of 0 known 20 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        cq::object* ts[20];
        std::set<uint256> unknown_set;
        std::set<std::shared_ptr<test_object>> unknown_refs;
        for (int i = 0; i < 20; ++i) {
            auto ob = test_object::make_random_unknown();
            unknown_refs.insert(ob);
            unknown_set.insert(ob->m_hash);
            ts[i] = ob.get();
        }

        auto pos = db->m_file->tell();
        db->refer(ts, 20);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 0);
        REQUIRE(unknown.size() == 20);
        REQUIRE(unknown == unknown_set);
    }

    SECTION("unordered set of 20 known 20 unknown references") {
        auto db = new_db();
        db->begin_segment(1);
        cq::object* ts[40];
        std::set<cq::id> known_set;
        std::set<std::shared_ptr<test_object>> known_refs;
        std::set<uint256> unknown_set;
        std::set<std::shared_ptr<test_object>> unknown_refs;
        for (int i = 0; i < 20; ++i) {
            auto ob = test_object::make_random_unknown();
            auto obid = db->store(ob.get());
            known_refs.insert(ob);
            known_set.insert(obid);
            ts[i] = ob.get();
        }
        for (int i = 20; i < 40; ++i) {
            auto ob = test_object::make_random_unknown();
            unknown_refs.insert(ob);
            unknown_set.insert(ob->m_hash);
            ts[i] = ob.get();
        }

        auto pos = db->m_file->tell();
        db->refer(ts, 40);
        db->m_file->seek(pos, SEEK_SET);
        std::set<cq::id> known;
        std::set<uint256> unknown;
        db->derefer(known, unknown);
        REQUIRE(known.size() == 20);
        REQUIRE(unknown.size() == 20);
        REQUIRE(known == known_set);
        REQUIRE(unknown == unknown_set);
    }

    //     /**
    //      * Segments are important positions in the stream of events which are referencable
    //      * from the follow-up header. Segments must be strictly increasing, but may include
    //      * gaps. This will automatically jump to the {last file, end of file} position and
    //      * disable `readonly` flag, (re-)enabling all write operations.
    //      */
    //     void begin_segment(id segment_id);

    SECTION("different cluster") {
        // CQ stores segments in their own clusters by default within 1024 segments, so if we
        // begin cluster 1024, it should result in a new file
        auto db = new_db();
        const auto& reg = db->get_registry();
        db->begin_segment(1);
        auto pos = db->m_file->tell();
        size_t filecount = db_file_count();
        REQUIRE(1 == reg.get_clusters().size());
        REQUIRE(0 == db->get_cluster());
        REQUIRE(1 == db->get_forward_index().get_segment_count());
        REQUIRE(pos == db->get_forward_index().get_segment_position(1));
        REQUIRE(1 == db->get_forward_index().get_first_segment());
        REQUIRE(1 == db->get_forward_index().get_last_segment());
        db->begin_segment(1024);
        auto pos2 = db->m_file->tell();
        REQUIRE(2 == reg.get_clusters().size());
        REQUIRE(1 == db->get_cluster());
        size_t filecount2 = db_file_count();
        REQUIRE(filecount2 == filecount + 2); // clister00001.cq, and cq.registry (created here because the first cluster change happens)

        REQUIRE(1 == db->get_forward_index().get_segment_count());
        REQUIRE(pos2 == db->get_forward_index().get_segment_position(1024));
        REQUIRE(1024 == db->get_forward_index().get_first_segment());
        REQUIRE(1024 == db->get_forward_index().get_last_segment());

        REQUIRE(1 == db->get_back_index().get_segment_count());
        REQUIRE(pos == db->get_back_index().get_segment_position(1));
        REQUIRE(1 == db->get_back_index().get_first_segment());
        REQUIRE(1 == db->get_back_index().get_last_segment());
    }

    //     /**
    //      * Seek to the {file, position} for the given segment.
    //      * Enables `readonly` flag, disabling all write operations.
    //      */
    //     void goto_segment(id segment_id);
    // };

    SECTION("segment jumping within one file") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob3 = test_object::make_random_unknown();

        db->begin_segment(1);
        db->store(ob.get());
        db->begin_segment(2);
        db->store(ob3.get());
        db->goto_segment(1);
        test_object ob2;
        db->load(&ob2);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
    }

    SECTION("segment jumping across two files") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob3 = test_object::make_random_unknown();

        db->begin_segment(1);
        db->store(ob.get());
        db->begin_segment(1025);
        db->store(ob3.get());
        db->goto_segment(1);
        test_object ob2;
        db->load(&ob2);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
    }

    SECTION("segment jumping across two files [long jump]") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob3 = test_object::make_random_unknown();

        db->begin_segment(1);
        db->store(ob.get());
        db->begin_segment(500000);
        db->store(ob3.get());
        db->goto_segment(1);
        REQUIRE(db->m_ic.m_cluster == 0);
        test_object ob2;
        db->load(&ob2);
        REQUIRE(ob->m_hash == ob2.m_hash);
        REQUIRE(ob->m_sid == ob2.m_sid);
        REQUIRE(*ob == ob2);
        REQUIRE(!db->m_ic.eof()); // although cluster 0 is eof, there is still segment 500000
        REQUIRE(db->m_ic.m_cluster == 500000/db->get_registry().m_cluster_size);
        db->load(&ob2);
        REQUIRE(ob3->m_hash == ob2.m_hash);
        REQUIRE(ob3->m_sid == ob2.m_sid);
    }

    SECTION("segment jumping across three files with gap") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto ob3 = test_object::make_random_unknown();

        db->begin_segment(1);
        db->store(ob.get());
        db->begin_segment(1025);
        db->store(ob2.get());
        db->begin_segment(100000);
        db->store(ob3.get());
        db->goto_segment(1);
        test_object obx;
        db->load(&obx);
        REQUIRE(ob->m_hash == obx.m_hash);
        REQUIRE(ob->m_sid == obx.m_sid);
        REQUIRE(*ob == obx);
        db->goto_segment(1025);
        db->load(&obx);
        REQUIRE(ob2->m_hash == obx.m_hash);
        REQUIRE(ob2->m_sid == obx.m_sid);
        REQUIRE(*ob2 == obx);
        db->goto_segment(100000);
        db->load(&obx);
        REQUIRE(ob3->m_hash == obx.m_hash);
        REQUIRE(ob3->m_sid == obx.m_sid);
        REQUIRE(*ob3 == obx);
    }

    SECTION("segment jumping across three files loading in between jumps") {
        auto db = new_db();
        auto ob = test_object::make_random_unknown();
        auto ob2 = test_object::make_random_unknown();
        auto ob3 = test_object::make_random_unknown();
        test_object obx;

        db->begin_segment(1);
        db->store(ob.get());
        db->goto_segment(1);
        db->load(&obx);
        REQUIRE(ob->m_hash == obx.m_hash);
        REQUIRE(ob->m_sid == obx.m_sid);
        REQUIRE(*ob == obx);
        db->begin_segment(1025);
        db->store(ob2.get());
        db->goto_segment(1);
        db->load(&obx);
        REQUIRE(ob->m_hash == obx.m_hash);
        REQUIRE(ob->m_sid == obx.m_sid);
        REQUIRE(*ob == obx);
        db->goto_segment(1025);
        db->load(&obx);
        REQUIRE(ob2->m_hash == obx.m_hash);
        REQUIRE(ob2->m_sid == obx.m_sid);
        REQUIRE(*ob2 == obx);
        db->begin_segment(100000);
        db->store(ob3.get());
        db->goto_segment(1025);
        db->load(&obx);
        REQUIRE(ob2->m_hash == obx.m_hash);
        REQUIRE(ob2->m_sid == obx.m_sid);
        REQUIRE(*ob2 == obx);
        db->goto_segment(1);
        db->load(&obx);
        REQUIRE(ob->m_hash == obx.m_hash);
        REQUIRE(ob->m_sid == obx.m_sid);
        REQUIRE(*ob == obx);
        db->goto_segment(100000);
        db->load(&obx);
        REQUIRE(ob3->m_hash == obx.m_hash);
        REQUIRE(ob3->m_sid == obx.m_sid);
        REQUIRE(*ob3 == obx);
    }
}
