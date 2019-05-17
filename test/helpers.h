#include <memory>
#include <cqdb/cq.h>

static inline cq::conditional* get_varint(uint8_t b, cq::id value) {
    switch (b) {
        case 1: return new cq::cond_varint<1>(value);
        case 2: return new cq::cond_varint<2>(value);
        case 3: return new cq::cond_varint<3>(value);
        case 4: return new cq::cond_varint<4>(value);
        case 5: return new cq::cond_varint<5>(value);
        case 6: return new cq::cond_varint<6>(value);
        case 7: return new cq::cond_varint<7>(value);
        default: throw std::runtime_error("invalid cond_varint bits");
    }
}

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

static const uint8_t cmd_reg = 0x00;    // reg <object>
static const uint8_t cmd_add = 0x01;    // add <object>
static const uint8_t cmd_del = 0x02;    // del <object|ref>
static const uint8_t cmd_mass = 0x03;   // mass <objects>
static const uint8_t cmd_nop = 0x04;    // nop

class test_chronology : public cq::chronology<test_object> {
public:
    using cq::chronology<test_object>::chronology;
    bool iterate() override {
        uint8_t cmd;
        bool known;
        uint256 hash;
        std::set<uint256> hash_set;
        if (!pop_event(cmd, known)) return false;
        switch (cmd) {
        case cmd_reg:
            pop_object();
            break;
        case cmd_add:
        case cmd_del:
            if (known) pop_reference(); else pop_reference(hash);
            break;
        case cmd_mass:
            pop_reference_hashes(hash_set);
            break;
        case cmd_nop: break;
        default:
            throw std::runtime_error("test_chronology encountered unknown command");
        }
        return true;
    }
};

inline std::shared_ptr<cq::db> open_db(const std::string& dbpath = "/tmp/cq-db-tests", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    auto rv = std::make_shared<cq::db>(dbpath, "cluster", 1008);
    rv->load();
    return rv;
}

inline std::shared_ptr<cq::db> new_db(const std::string& dbpath = "/tmp/cq-db-tests") {
    return open_db(dbpath, true);
}

inline size_t db_file_count(const std::string& dbpath = "/tmp/cq-db-tests") {
    std::vector<std::string> l;
    cq::listdir(dbpath, l);
    return l.size();
}

inline std::shared_ptr<test_chronology> open_chronology(const std::string& dbpath = "/tmp/cq-db-tests", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    auto rv = std::make_shared<test_chronology>(dbpath, "cluster", 1008);
    rv->load();
    return rv;
}

inline std::shared_ptr<test_chronology> new_chronology(const std::string& dbpath = "/tmp/cq-db-tests") {
    return open_chronology(dbpath, true);
}

inline size_t chronology_file_count(const std::string& dbpath = "/tmp/cq-db-tests") {
    return db_file_count(dbpath);
}
