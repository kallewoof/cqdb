#include "catch.hpp"

#include <memory>
#include <cqdb/cq.h>

struct test_object : public cq::object {
    using cq::object::object;
    void serialize(cq::serializer* stream) const override {
        m_hash.Serialize(*stream);
    }
    void deserialize(cq::serializer* stream) override {
        m_hash.Unserialize(*stream);
    }
    static std::shared_ptr<test_object> make_random_unknown() {
        uint256 hash;
        cq::randomize(hash.begin(), 32);
        return std::make_shared<test_object>(hash);
    }
};

inline std::shared_ptr<cq::db> open_db(const std::string& dbpath = "/tmp/cq-db-tests", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    return std::make_shared<cq::db>(dbpath, "cluster", 1008);
}

inline std::shared_ptr<cq::db> new_db(const std::string& dbpath = "/tmp/cq-db-tests") {
    return open_db(dbpath, true);
}

inline size_t db_file_count(const std::string& dbpath = "/tmp/cq-db-tests") {
    std::vector<std::string> l;
    cq::listdir(dbpath, l);
    return l.size();
}

inline std::shared_ptr<cq::chronology<test_object>> open_chronology(const std::string& dbpath = "/tmp/cq-db-tests", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    return std::make_shared<cq::chronology<test_object>>(dbpath, "cluster", 1008);
}

inline std::shared_ptr<cq::chronology<test_object>> new_chronology(const std::string& dbpath = "/tmp/cq-db-tests") {
    return open_chronology(dbpath, true);
}

inline size_t chronology_file_count(const std::string& dbpath = "/tmp/cq-db-tests") {
    return db_file_count(dbpath);
}

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

TEST_CASE("Registry", "[registry]") {
    SECTION("empty") {
        cq::registry empty(2016);
        REQUIRE(empty.get_clusters().size() == 0);
        cq::chv_stream stream;
        empty.serialize(&stream);
        // should be able to serialize above registry in 6 bytes (size (1) + cluster size (4) + tip (1))
        REQUIRE(stream.tell() == 6);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(empty == reg2);
    }

    SECTION("one entry") {
        cq::registry one(2016);
        one.open_cluster_for_segment(1 * 2016);
        REQUIRE(one.get_clusters().size() == 1);
        cq::chv_stream stream;
        one.serialize(&stream);
        // should be able to serialize above registry in 7 bytes (size (1) + entry (1) + cluster size (4) + tip (1))
        REQUIRE(stream.tell() == 7);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(one == reg2);
    }

    SECTION("two entries") {
        cq::registry reg(2016);
        reg.open_cluster_for_segment(1 * 2016);
        reg.open_cluster_for_segment(128 * 2016);
        REQUIRE(reg.get_clusters().size() == 2);
        cq::chv_stream stream;
        reg.serialize(&stream);
        // should be able to serialize above registry in 8 bytes (size (1) + entries (1 + 1) + cluster size (4) + tip (1))
        // note that entry 2 is 128, but relative, so 127
        REQUIRE(stream.tell() == 8);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(reg == reg2);
    }

    SECTION("opening clusters for segments") {
        cq::registry reg(2016);
        REQUIRE(reg.get_clusters().size() == 0);
        REQUIRE(reg.open_cluster_for_segment(2015) == 0);
        REQUIRE(reg.get_clusters().size() == 1);
        REQUIRE(reg.open_cluster_for_segment(2016) == 1);
        REQUIRE(reg.get_clusters().size() == 2);
        cq::chv_stream stream;
        reg.serialize(&stream);
        // should be able to serialize above registry in 8 bytes (size (1) + entries (1 + 1) + cluster size (4) + tip (1))
        // why 1 + 1 despite 2015? because we are storing the *cluster numbers* not the segment ids, i.e. 0 and 1 not 2015 and 2016
        // why 1 for tip, despite it being 2016? because tip is serialized as (tip - cluster * cluster_size) i.e. (2016 - 1 * 2016) = 0, here
        REQUIRE(stream.tell() == 8);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(reg == reg2);
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
        hdr.serialize(&stm);
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
        hdr.serialize(&stm);
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
        hdr.serialize(&stm);
        stm.seek(0, SEEK_SET);
        cq::header hdr2(0, &stm);
        REQUIRE(2 == hdr2.get_segment_count());
        REQUIRE(2 == hdr2.get_segment_position(1));
        REQUIRE(3 == hdr2.get_segment_position(999999));
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
        // attempting to store ob should throw a db_error, because we have not yet begun a segment
        REQUIRE_THROWS_AS(db->store(ob.get()), cq::db_error);
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
        REQUIRE(1 == db->get_header()->get_segment_count());
        REQUIRE(pos == db->get_header()->get_segment_position(1));
        REQUIRE(1 == db->get_header()->get_first_segment());
        REQUIRE(1 == db->get_header()->get_last_segment());
        db->begin_segment(1024);
        auto pos2 = db->m_file->tell();
        REQUIRE(2 == reg.get_clusters().size());
        REQUIRE(1 == db->get_cluster());
        size_t filecount2 = db_file_count();
        REQUIRE(filecount2 == filecount + 1);

        REQUIRE(1 == db->get_header()->get_segment_count());
        REQUIRE(pos2 == db->get_header()->get_segment_position(1024));
        REQUIRE(1024 == db->get_header()->get_first_segment());
        REQUIRE(1024 == db->get_header()->get_last_segment());

        REQUIRE(1 == db->get_footer()->get_segment_count());
        REQUIRE(pos == db->get_footer()->get_segment_position(1));
        REQUIRE(1 == db->get_footer()->get_first_segment());
        REQUIRE(1 == db->get_footer()->get_last_segment());
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
            cq::db db(dbpath, "chronology", 1008);
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

    static const uint8_t cmd_add = 0x01;    // add <object>
    static const uint8_t cmd_del = 0x02;    // del <object|ref>
    static const uint8_t cmd_nop = 0x03;    // nop

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
            chron->push_event(1557974775, cmd_add, ob, false);
            obid = ob->m_sid;
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false);
            chron->push_event(1557974776, cmd_add, ob2, false);
            obid = ob->m_sid;
            obid2 = ob2->m_sid;
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            std::shared_ptr<test_object> ob = chron->pop_object();
            REQUIRE(ob->m_hash == obhash);
            REQUIRE(ob->m_sid == obid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false);
            obid = ob->m_sid;
            chron->push_event(1557974776, cmd_del, ob, false);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false);
            chron->push_event(1557974776, cmd_add, ob2, false);
            chron->push_event(1557974777, cmd_del, ob, false);
            chron->push_event(1557974778, cmd_del, ob2, false);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(chron->current_time == 1557974775);
            REQUIRE(!known);
            obx = chron->pop_object();
            REQUIRE(obx->m_hash == ob->m_hash);
            REQUIRE(obx->m_sid == ob->m_sid);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false);
            chron->push_event(1557974776, cmd_add, ob2);
            chron->push_event(1557974777, cmd_del, ob);
            chron->push_event(1557974778, cmd_del, ob2);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            std::shared_ptr<test_object> obx;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_add, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_del, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_del, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
            chron->push_event(1557974775, cmd_add, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_add, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_del, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_del, std::set<std::shared_ptr<test_object>>{ob, ob2});
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
            chron->push_event(1557974775, cmd_add, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_add, ob2, false); // write full ref to make ob known
            chron->push_event(1557974777, cmd_del, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974776);
            obx = chron->pop_object();
            REQUIRE(*obx == *ob2);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
            chron->push_event(1557974775, cmd_add, ob, false); // write full ref to make ob known
            chron->push_event(1557974776, cmd_del, set);
        }
        {
            auto chron = open_chronology();
            chron->m_file->seek(pos, SEEK_SET);
            uint8_t cmd;
            bool known;
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_add == cmd);
            REQUIRE(!known);
            REQUIRE(chron->current_time == 1557974775);
            std::shared_ptr<test_object> obx;
            obx = chron->pop_object();
            REQUIRE(*obx == *ob);
            REQUIRE(true == chron->pop_event(cmd, known));
            REQUIRE(cmd_del == cmd);
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
}
