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
        cq::uint256 hash;
        cq::randomize(hash.begin(), 32);
        return std::make_shared<test_object>(hash);
    }
};

class test_registry_delegate : public cq::registry_delegate {
public:
    virtual void registry_closing_cluster(cq::id cluster) override {}
    virtual void registry_opened_cluster(cq::id cluster, cq::file* file) override {}
    virtual bool registry_iterate(cq::file* file) override { return false; }
};

static const uint8_t cmd_reg = 0x00;    // reg <object>
static const uint8_t cmd_add = 0x01;    // add <object>
static const uint8_t cmd_del = 0x02;    // del <object|ref>
static const uint8_t cmd_mass = 0x03;   // mass <objects>
static const uint8_t cmd_mass_compressed = 0x04; // mass_compressed <objects>
static const uint8_t cmd_nop = 0x05;    // nop

class test_chronology : public cq::chronology<test_object> {
public:
    using cq::chronology<test_object>::chronology;
    bool registry_iterate(cq::file* file) override {
        m_file = file;
        uint8_t cmd;
        bool known;
        cq::uint256 hash;
        std::set<cq::uint256> hash_set;
        std::vector<cq::uint256> hash_vec;
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
        case cmd_mass_compressed:
            decompress(file, hash_vec);
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
    CHRON_SET_REFLECTION(rv, std::make_shared<test_chronology>(dbpath, "cluster", 1008, true));
    return rv;
}

inline std::shared_ptr<test_chronology> new_chronology(const std::string& dbpath = "/tmp/cq-db-tests") {
    return open_chronology(dbpath, true);
}

inline size_t chronology_file_count(const std::string& dbpath = "/tmp/cq-db-tests") {
    return db_file_count(dbpath);
}

struct test_cluster_delegate : public cq::cluster_delegate {
private:
    cq::id m_last_cluster = cq::nullid;
    std::vector<cq::id> m_clusters;
    std::map<cq::id,cq::id> m_cluster_hoplist;
    std::string m_dbpath;
    std::string m_prefix;
public:
    bool* death_ptr{nullptr};
    test_cluster_delegate(const std::string& dbpath, const std::string& prefix) : m_dbpath(dbpath), m_prefix(prefix) {
    }
    ~test_cluster_delegate() {
        if (death_ptr) *death_ptr = true;
    }
    test_cluster_delegate& operator+=(cq::id cluster) {
        if (m_last_cluster != cq::nullid) {
            assert(cluster > m_last_cluster);
            m_cluster_hoplist[m_last_cluster] = cluster;
        }
        m_last_cluster = cluster;
        m_clusters.push_back(cluster);
        return *this;
    }

    size_t size() const { return m_clusters.size(); }

    cq::id const operator[](size_t index) const {
        return m_clusters.at(index);
    }

    virtual cq::id cluster_next(cq::id cluster) override {
        if (cluster == cq::nullid) {
            return m_clusters.size() ? m_clusters.at(0) : cq::nullid;
        }
        return m_cluster_hoplist.count(cluster) ? m_cluster_hoplist.at(cluster) : cq::nullid;
    }

    virtual cq::id cluster_last(bool open_for_writing) override {
        if (open_for_writing && m_last_cluster == cq::nullid) operator+=(0);
        return m_last_cluster;
    }

    virtual std::string cluster_path(cq::id cluster) override {
        char clu[6];
        sprintf(clu, "%05lld", cluster);
        return m_dbpath + "/" + m_prefix + clu + ".cq";
    }

    virtual void cluster_will_close(cq::id cluster) override {}

    virtual void cluster_opened(cq::id cluster, cq::file* file) override {
        if (m_last_cluster == cq::nullid || m_last_cluster < cluster) {
            operator+=(cluster);
        }
    }
};

inline std::pair<std::shared_ptr<test_cluster_delegate>,std::shared_ptr<cq::cluster>> open_cluster(std::shared_ptr<test_cluster_delegate>* delegate = nullptr, const std::string& dbpath = "/tmp/cq-io-test-cluster", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    cq::mkdir(dbpath); // create path here as it is not created by the cluster
    if (delegate && !delegate->get()) delegate->reset(new test_cluster_delegate(dbpath, "cluster"));
    auto cd = delegate ? *delegate : std::make_shared<test_cluster_delegate>(dbpath, "cluster");
    auto c = std::make_shared<cq::cluster>(cd.get(), false);
    return std::make_pair<std::shared_ptr<test_cluster_delegate>,std::shared_ptr<cq::cluster>>(std::move(cd), std::move(c));
}

inline std::pair<std::shared_ptr<test_cluster_delegate>,std::shared_ptr<cq::cluster>> new_cluster(std::shared_ptr<test_cluster_delegate>* delegate = nullptr, const std::string& dbpath = "/tmp/cq-io-test-cluster") {
    return open_cluster(delegate, dbpath, true);
}

inline size_t cluster_file_count(const std::string& dbpath = "/tmp/cq-io-test-cluster") {
    return db_file_count(dbpath);
}

struct test_index : public cq::serializable {
    cq::id index_id = cq::nullid;
    void serialize(cq::serializer* stream) const override { stream->w(index_id); }
    void deserialize(cq::serializer* stream)  override    { stream->r(index_id); }
};

struct test_indexed_cluster_delegate : public cq::indexed_cluster_delegate {
private:
    cq::id m_last_cluster = cq::nullid;
    std::vector<cq::id> m_clusters;
    std::map<cq::id,cq::id> m_cluster_hoplist;
    std::string m_dbpath;
    std::string m_prefix;
    cq::file* m_file;
    test_index m_fwd;
    test_index m_bk;
public:
    bool* death_ptr{nullptr};
    test_indexed_cluster_delegate(const std::string& dbpath, const std::string& prefix) : m_dbpath(dbpath), m_prefix(prefix) {
    }
    ~test_indexed_cluster_delegate() {
        if (death_ptr) *death_ptr = true;
    }
    test_indexed_cluster_delegate& operator+=(cq::id cluster) {
        if (m_last_cluster != cq::nullid) {
            assert(cluster > m_last_cluster);
            m_cluster_hoplist[m_last_cluster] = cluster;
        }
        m_last_cluster = cluster;
        m_clusters.push_back(cluster);
        return *this;
    }

    size_t size() const { return m_clusters.size(); }

    cq::id const operator[](size_t index) const {
        return m_clusters.at(index);
    }

    virtual cq::id cluster_next(cq::id cluster) override {
        if (cluster == cq::nullid) {
            return m_clusters.size() ? m_clusters.at(0) : cq::nullid;
        }
        return m_cluster_hoplist.count(cluster) ? m_cluster_hoplist.at(cluster) : cq::nullid;
    }

    virtual cq::id cluster_last(bool open_for_writing) override {
        if (open_for_writing && m_last_cluster == cq::nullid) operator+=(0);
        return m_last_cluster;
    }

    virtual std::string cluster_path(cq::id cluster) override {
        char clu[6];
        sprintf(clu, "%05lld", cluster);
        return m_dbpath + "/" + m_prefix + clu + ".cq";
    }

    virtual void cluster_will_close(cq::id cluster) override {
    }

    virtual void cluster_opened(cq::id cluster, cq::file* file) override {
        if (m_last_cluster == cq::nullid || m_last_cluster < cluster) {
            operator+=(cluster);
        }
    }

    virtual void cluster_write_forward_index(cq::id cluster, cq::file* file) override {
        REQUIRE(file->get_path() == cluster_path(cluster));
        REQUIRE(m_fwd.index_id == cluster);
        *file << m_fwd;
    }

    virtual void cluster_read_forward_index(cq::id cluster, cq::file* file) override {
        REQUIRE(file->get_path() == cluster_path(cluster));
        *file >> m_fwd;
        REQUIRE(m_fwd.index_id == cluster);
    }

    virtual void cluster_clear_forward_index(cq::id cluster) override {
        m_fwd.index_id = cluster;
    }

    virtual void cluster_read_back_index(cq::id cluster, cq::file* file) override {
        REQUIRE(file->get_path() == cluster_path(cluster));
        *file >> m_bk;
        REQUIRE(m_bk.index_id == cluster);
    }

    virtual void cluster_clear_and_write_back_index(cq::id cluster, cq::file* file) override {
        REQUIRE(file->get_path() == cluster_path(cluster));
        m_bk.index_id = cluster;
        *file << m_bk;
    }

    virtual bool cluster_iterate(cq::id cluster, cq::file* file) override {
        file->seek(0, SEEK_END);
        return false;
    }
};

struct test_indexed_cluster_ctr {
    std::shared_ptr<test_indexed_cluster_delegate> m_delegate;
    cq::indexed_cluster m_ic;
    test_indexed_cluster_ctr(std::shared_ptr<test_indexed_cluster_delegate> delegate, bool readonly) : m_delegate(delegate), m_ic(delegate.get(), readonly) {}
    ~test_indexed_cluster_ctr() {
        m_ic.close();
    }
};

inline std::pair<std::shared_ptr<test_indexed_cluster_delegate>,std::shared_ptr<test_indexed_cluster_ctr>> open_indexed_cluster(std::shared_ptr<test_indexed_cluster_delegate>* delegate = nullptr, const std::string& dbpath = "/tmp/cq-io-test-indexed-cluster", bool reset = false) {
    if (reset) cq::rmdir_r(dbpath);
    cq::mkdir(dbpath); // create path here as it is not created by the cluster
    if (delegate && !delegate->get()) delegate->reset(new test_indexed_cluster_delegate(dbpath, "cluster"));
    auto cd = delegate ? *delegate : std::make_shared<test_indexed_cluster_delegate>(dbpath, "cluster");
    auto c = std::make_shared<test_indexed_cluster_ctr>(cd, false);
    return std::make_pair<std::shared_ptr<test_indexed_cluster_delegate>,std::shared_ptr<test_indexed_cluster_ctr>>(std::move(cd), std::move(c));
}

inline std::pair<std::shared_ptr<test_indexed_cluster_delegate>,std::shared_ptr<test_indexed_cluster_ctr>> new_indexed_cluster(std::shared_ptr<test_indexed_cluster_delegate>* delegate = nullptr, const std::string& dbpath = "/tmp/cq-io-test-indexed-cluster") {
    return open_indexed_cluster(delegate, dbpath, true);
}

inline size_t indexed_cluster_file_count(const std::string& dbpath = "/tmp/cq-io-test-indexed-cluster") {
    return db_file_count(dbpath);
}
