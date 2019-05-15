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
        REQUIRE(empty.m_clusters.m.size() == 0);
        cq::chv_stream stream;
        empty.serialize(&stream);
        // should be able to serialize above registry in 5 bytes (size (1) + cluster size (4))
        REQUIRE(stream.tell() == 5);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(empty == reg2);
    }

    SECTION("one entry") {
        cq::registry one(2016);
        one.m_clusters.m.insert(1);
        REQUIRE(one.m_clusters.m.size() == 1);
        cq::chv_stream stream;
        one.serialize(&stream);
        // should be able to serialize above registry in 6 bytes (size (1) + entry (1) + cluster size (4))
        REQUIRE(stream.tell() == 6);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(one == reg2);
    }

    SECTION("two entries") {
        cq::registry reg(2016);
        reg.m_clusters.m.insert(1);
        reg.m_clusters.m.insert(128);
        REQUIRE(reg.m_clusters.m.size() == 2);
        cq::chv_stream stream;
        reg.serialize(&stream);
        // should be able to serialize above registry in 7 bytes (size (1) + entries (1 + 1) + cluster size (4))
        // note that entry 2 is 128, but relative, so 127
        REQUIRE(stream.tell() == 7);
        cq::registry reg2;
        stream.seek(0, SEEK_SET);
        reg2.deserialize(&stream);
        REQUIRE(reg == reg2);
    }

    SECTION("opening clusters for segments") {
        cq::registry reg(2016);
        REQUIRE(reg.m_clusters.m.size() == 0);
        REQUIRE(reg.open_cluster_for_segment(2015) == 0);
        REQUIRE(reg.m_clusters.m.size() == 1);
        REQUIRE(reg.open_cluster_for_segment(2016) == 1);
        REQUIRE(reg.m_clusters.m.size() == 2);
        cq::chv_stream stream;
        reg.serialize(&stream);
        // should be able to serialize above registry in 7 bytes (size (1) + entries (1 + 1) + cluster size (4))
        // why 1 + 1 despite 2015? because we are storing the *cluster numbers* not the segment ids, i.e. 0 and 1 not 2015 and 2016
        REQUIRE(stream.tell() == 7);
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
        REQUIRE(1 == reg.m_clusters.size());
        REQUIRE(0 == db->get_cluster());
        REQUIRE(1 == db->get_header()->get_segment_count());
        REQUIRE(pos == db->get_header()->get_segment_position(1));
        REQUIRE(1 == db->get_header()->get_first_segment());
        REQUIRE(1 == db->get_header()->get_last_segment());
        db->begin_segment(1024);
        auto pos2 = db->m_file->tell();
        REQUIRE(2 == reg.m_clusters.size());
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
    // template<typename T>
    // class chronology : public db {
    // public:
    //     using db::m_file;
    //     long current_time;
    //     long pending_time;
    //     uint8_t pending_cmd;
    //     static const uint8_t none = 0xff;
    //     std::map<id, std::shared_ptr<T>> m_dictionary;
    //     std::map<uint256, id> m_references;

    //     chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    //     : current_time(0)
    //     , pending_time(0)
    //     , pending_cmd(0xff)
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

    //     // command header is constructed as follows:
    //     // bit range   purpose
    //     // ----------- --------------------------------------------------------
    //     // 0..4        protocol command space (0x00..0x0f, where 0x0e = "reference known", 0x0f = "reference unknown")
    //     // 5           KNOWN. Reference is known (1; id varint) or unknown (0; FQR)
    //     // 6..7        TIME_REL. Time relative value (00=same as last, 01=1 second later, 10=2 seconds later, 11=varint relative to previous timestamp)

    //     inline uint8_t header(uint8_t cmd, long time, bool known) {
    //         assert(cmd == (cmd & 0x0f));
    //         return cmd | (known << 5) | time_rel_bits(time - current_time);
    //     }

    //     void prepare_header(uint8_t cmd, long time) {
    //         assert(cmd == (cmd & 0x0f));
    //         if (pending_cmd != none) {
    //             // write pending command byte
    //             uint8_t u8 = header(pending_cmd, pending_time, false);
    //             m_file << u8;
    //             _write_time(u8, current_time, pending_time);
    //         }
    //         pending_cmd = cmd;
    //         pending_time = time;
    //     }

    //     void mark(id known_id) {
    //         if (pending_cmd != none) {
    //             uint8_t u8 = header(pending_cmd, pending_time, true);
    //             m_file << u8;
    //             _write_time(u8, current_time, pending_time);
    //             pending_cmd = none;
    //             return;
    //         }
    //         uint8_t u8 = header(0x0e, current_time, true);
    //         // no _write_time() because current_time == current_time, so relative time is always 0
    //         m_file << u8;
    //     }

    //     void mark(const uint256& unknown_ref) {
    //         if (pending_cmd != none) {
    //             uint8_t u8 = header(pending_cmd, pending_time, false);
    //             m_file << u8;
    //             _write_time(u8, current_time, pending_time);
    //             pending_cmd = none;
    //             return;
    //         }
    //         uint8_t u8 = header(0x0f, current_time, false);
    //         // no _write_time() because current_time == current_time, so relative time is always 0
    //         m_file << u8;
    //     }

    //     // repositories automate the process of tracking which objects have been stored (and thus can be referred to by their id)
    //     // and which objects are unknown

    //     // referencing an object has the following possible combinations:
    //     //      id = id available
    //     //      FQR = FQR (hash) available
    //     //      T = object available
    //     //      dct = object found in m_dictionary
    //     //      ref = id found in m_references
    //     //
    //     //      id  FQR T   dct ref Outcome
    //     //      === === === === === =============================================================================================================
    //     //      no  no  no  -   -   unreferencable
    //     //      no  yes no  -   no  reference unknown object with given FQR
    //     //      no  yes no  -   yes see [ yes - - yes - ] (ref provides id and infers dct)
    //     //      no  yes yes -   -   assign id to object, register known object to stream, add to dictionary/references
    //     //      yes no  no  no  -   illegal reference; throws chronology_error (future 'repair' mode may load and reference unknown here instead)
    //     //      yes -   -   yes -   reference known object with given id
    //     //      yes -   yes no  -   id is outdated (object was purged); re-assign new id and re-register known object to stream, add to dict/refs
    //     //      === === === === === =============================================================================================================

    //     inline void register_object(const std::shared_ptr<T>& object) {
    //         store(object.get());
    //     }

    //     inline void refer(const uint256& hash, id sid) {
    //     //      yes -   -   yes -   reference known object with given id
    //     //      yes -   yes no  -   id is outdated (object was purged); re-assign new id and re-register known object to stream, add to dict/refs
    //         if (m_dictionary.count(sid)) {
    //             // reference known object with given id
    //             return mark(sid);
    //         }

    //     }

    //     void refer(const uint256& hash) {
    //         if (m_references.count(hash)) return refer(hash, m_references.at(hash));
    //     }
    // };
}
