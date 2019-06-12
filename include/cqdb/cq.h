#ifndef included_cq_cq_h_
#define included_cq_cq_h_

#include <cqdb/config.h>

#include <string>
#include <map>
#include <memory>
#include <ios>

#include <cstdlib>
#include <cstdio>
#include <assert.h>

#include <cqdb/io.h>

#ifdef USE_REFLECTION
#   define CHRON_DOT(chron) chron->period()
#   define CHRON_SET_REFLECTION(chron, reflection...) chron->enable_reflection(reflection)
#else
#   define CHRON_DOT(chron)
#   define CHRON_SET_REFLECTION(chron, reflection...)
#endif

extern "C" { void libcqdb_is_present(void); } // hello autotools, pleased to meat you

namespace cq {

class db_error : public std::runtime_error { public: explicit db_error(const std::string& str) : std::runtime_error(str) {} };
class chronology_error : public std::runtime_error { public: explicit chronology_error(const std::string& str) : std::runtime_error(str) {} };

#define OBREF(known, x) if (x.get()) { if (known) { refer(x.get()); } else { refer(x->m_hash); } }
#define FERBO(known, u256) known ? m_dictionary.at(pop_reference())->m_hash : pop_reference(u256)

constexpr id unknownid = 0x0;

template<typename H> class object : public serializable {
public:
    id m_sid;
    H m_hash;
    compressor<H>* m_compressor;
    object(compressor<H>* compressor, const H& hash) : object(compressor, unknownid, hash) {}
    object(compressor<H>* compressor, id sid = unknownid, const H& hash = H()) : m_sid(sid), m_hash(hash), m_compressor(compressor) {}
    bool operator==(const object& other) const {
        return m_hash == other.m_hash;
    }
    inline bool operator!=(const object& other) const { return !operator==(other); }
    bool operator<(const object& other) const { return m_hash < other.m_hash; }
};

static const uint8_t HEADER_VERSION = 1;

class header : public serializable {
private:
    uint8_t m_version;                  //!< CQ version byte
    /**
     * A map of segment ID : file position, where the segment ID in bitcoin's case refers to the block height.
     */
    incmap m_segments;
public:
    id m_cluster;

    header(uint8_t version, id cluster);
    header(id cluster, serializer* stream);
    void reset(uint8_t version, id cluster);

    prepare_for_serialization();

    void adopt(const header& other) {
        assert(m_version == other.m_version);
        m_segments = other.m_segments;
        m_cluster = other.m_cluster;
    }

    void mark_segment(id segment, id position);
    id get_segment_position(id segment) const;
    bool has_segment(id segment) const;
    id get_first_segment() const;
    id get_last_segment() const;
    size_t get_segment_count() const;
    uint8_t get_version() const { return m_version; }
    std::string to_string() const {
        std::string s = "<cluster=" + std::to_string(m_cluster) + ">(\n";
        char v[256];
        for (const auto& kv : m_segments.m) {
            sprintf(v, "   %lld = %lld\n", kv.first, kv.second);
            s += v;
        }
        return s + ")";
    }
};

class registry_delegate {
public:
    virtual void registry_closing_cluster(id cluster) =0;
    virtual void registry_opened_cluster(id cluster, file* file) =0;
    virtual bool registry_iterate(file* file) =0;
};

class registry : public serializable, public indexed_cluster_delegate {
private:
    std::string m_dbpath;
    std::string m_prefix;
    /**
     * A list of existing clusters in the registry.
     */
    unordered_set m_clusters;
public:
    uint32_t m_cluster_size;
    id m_tip;
    registry_delegate* m_delegate;
    header m_forward_index;    // this is the worked-on header for the current (unfinished) cluster
    header m_back_index;       // this is the readonly header referencing the previous cluster, if any
    id m_current_cluster;

    registry(registry_delegate* delegate, const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    :   m_dbpath(dbpath)
    ,   m_prefix(prefix)
    ,   m_cluster_size(cluster_size)
    ,   m_tip(0)
    ,   m_delegate(delegate)
    ,   m_forward_index(HEADER_VERSION, nullid)
    ,   m_back_index(HEADER_VERSION, nullid)
    ,   m_current_cluster(nullid)
    {}

    prepare_for_serialization();

    void adopt(const registry& other) {
        assert(m_dbpath == other.m_dbpath);
        assert(m_prefix == other.m_prefix);
        assert(m_cluster_size == other.m_cluster_size);
        m_clusters.m = other.m_clusters.m;
        m_tip = other.m_tip;
        m_forward_index.adopt(other.m_forward_index);
        m_back_index.adopt(other.m_back_index);
        m_current_cluster = other.m_current_cluster;
    }

    // cluster delegation
    virtual id cluster_next(id cluster) override;
    virtual id cluster_last(bool open_for_writing) override;
    virtual std::string cluster_path(id cluster) override;
    virtual void cluster_will_close(id cluster) override;
    virtual void cluster_opened(id cluster, file* file) override;
    virtual void cluster_write_forward_index(id cluster, file* file) override;
    virtual void cluster_read_forward_index(id cluster, file* file) override;
    virtual void cluster_clear_forward_index(id cluster) override;
    virtual void cluster_read_back_index(id cluster, file* file) override;
    virtual void cluster_clear_and_write_back_index(id cluster, file* file) override;
    virtual bool cluster_iterate(id cluster, file* file) override { return m_delegate->registry_iterate(file); }

    id prepare_cluster_for_segment(id segment);
    inline bool operator==(const registry& other) const { return m_cluster_size == other.m_cluster_size && m_clusters == other.m_clusters && m_tip == other.m_tip; }
    inline const unordered_set& get_clusters() const { return m_clusters; }
};

template<typename H> class db : public registry_delegate {
protected:
    const std::string m_dbpath;
    const std::string m_prefix;

    void open(id cluster, bool readonly);
    void reopen();

    void close();
    bool m_readonly;

public:
    file* m_file;
    registry m_reg;
    indexed_cluster m_ic;

    // struction
    db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024, bool readonly = false);
    virtual ~db();
    void load();

    // registry delegate
    virtual void registry_closing_cluster(id cluster) override;
    virtual void registry_opened_cluster(id cluster, file* file) override;
    virtual bool registry_iterate(file* file) override { file->seek(0, SEEK_END); return false; }

    // registry
    id store(object<H>* t);                    // writes object to disk and returns its absolute id
    void load(object<H>* t);                   // reads object from disk at current position
    void fetch(object<H>* t, id i);            // fetches object with obid 'i' into *t from disk
    void refer(id sid);                     // writes a reference to the object with the given sid
    void refer(object<H>* t);                  // writes a reference to t to disk (known)
    id derefer();                           // reads a reference id from disk
    void refer(const H& hash);       // write a reference to an unknown object to disk
    H& derefer(H& hash);      // reads a reference to an unknown object from disk

    void refer(object<H>** ts, size_t sz);     // writes an unordered set of references to sz number of objects
    void derefer(std::set<id>& known        // reads an unordered set of known/unknown references from disk
               , std::set<H>& unknown);

    inline const registry& get_registry() const { return m_reg; }
    inline const id& get_cluster() const { return m_ic.m_cluster; }
    inline const header& get_forward_index() const { return m_reg.m_forward_index; }
    inline const header& get_back_index() const { return m_reg.m_back_index; }
    inline std::string stell() const { char v[256]; sprintf(v, "%lld:%ld", m_ic.m_cluster, m_ic.m_file ? m_ic.m_file->tell() : -1); return v; }

    /**
     * Segments are important positions in the stream of events which are referencable
     * from the follow-up header. Segments must be strictly increasing, but may include
     * gaps. This will automatically jump to the {last file, end of file} position and
     * disable `readonly` flag, (re-)enabling all write operations.
     */
    virtual void begin_segment(id segment_id);

    /**
     * Seek to the {file, position} for the given segment.
     * Enables `readonly` flag, disabling all write operations.
     */
    virtual void goto_segment(id segment_id);

    /**
     * Rewind to the very beginning of the data.
     */
    inline void rewind() { goto_segment(*m_reg.get_clusters().m.begin()); }

    void flush();
};

inline uint8_t time_rel_value(uint8_t cmd) { return cmd >> 6; }
inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

#define _read_time(H, t, current_time, timerel) \
    t = current_time + timerel + (timerel > 2 ? varint::load(m_file) : 0)

#define read_cmd_time(H, u8, cmd, known, timerel, time, current_time) do { \
        u8 = m_file->get_uint8(); \
        cmd = (u8 & 0x1f); /* 0b0001 1111 */ \
        known = 0 != (u8 & 0x20); \
        timerel = time_rel_value(u8); \
        _read_time(H, time, current_time, timerel); \
    } while(0)

#define _write_time(H, rel, current_time, write_time) do { \
        if (time_rel_value(rel) > 2) { \
            uint64_t tfull = uint64_t(write_time - time_rel_value(rel) - current_time); \
            varint(tfull).serialize(m_file); \
            current_time = write_time; \
        } else { \
            current_time += time_rel_value(rel);\
        }\
    } while (0)

/**
 * The chronology class introduces a timeline to the database.
 *
 * Let's say we have two objects "foo" and "bar" (both unknown at t=0), and the commands
 * 'enter [ob]', 'leave [ob]', 'graduate [array]', and 'jump [void]'. Our timeline goes as follows:
 *      t (time):   object:     event:
 *      1557811967  foo         first seen entering the system
 *      1557811968  bar         first seen entering the system
 *      1557811998  -           jump
 *      1557812000  bar         leave
 *      1557812001  [foo, bar]  graduate
 * A chronology would store this as a series of events with relative timestamps, in the following fashion:
 *      Δt          known:      command:    payload:
 *      1557811967  false       enter       foo.id = store(foo)
 *      1           false       enter       bar.id = store(bar)
 *      30          -           jump
 *      2           true        leave       bar.id
 *      1           -           graduate    [foo, bar]
 * The application would perform the above series as follows:
 *      chronology chron(...);
 *      std::shared_ptr<object> foo("foo");
 *      std::shared_ptr<object> bar("bar");
 *      chron.push_event(1557811967, ENTER, foo, false);
 *      chron.push_event(1557811968, ENTER, bar, false);
 *      chron.push_event(1557811998, JUMP);
 *      chron.push_event(1557812000, LEAVE, bar);
 *      chron.push_event(1557812001, GRADUATE, std::set<std::shared_ptr<object>>{foo, bar});
 * Later, when trying to relive the events, the application would:
 *      pop_event(cmd, known);  [cmd:ENTER; !known] -> pop_object(foo)
 *      -'-;                    [cmd:ENTER; !known] -> pop_object(bar)
 *      -'-;                    [cmd:JUMP]          -> [...]
 *      -'-;                    [cmd:LEAVE; known]  -> ref = pop_reference();               m_references.at(ref) == bar
 *      -'-;                    [cmd:GRADUATE]      -> pop_references(known, unknown);      known == [foo, bar]
 */
template<typename H, typename T>
class chronology : public db<H>, public compressor<H> {
protected:
public:
    using db<H>::derefer;
    using db<H>::refer;
    using db<H>::m_reg;
    using db<H>::m_file;
    using db<H>::m_ic;
    long m_current_time;
    std::map<id, std::shared_ptr<T>> m_dictionary;
    std::map<H, id> m_references;

#ifdef USE_REFLECTION
    std::shared_ptr<chronology> m_reflection; // debug tool used to assert that serialized data deserializes to itself

    inline void enable_reflection(std::shared_ptr<chronology> reflection) {
        if (!m_file) load();
        if (!reflection->m_readonly) throw chronology_error("invalid reflection (must be readonly)");
        if (reflection->m_dbpath != m_dbpath) throw chronology_error("invalid reflection (dbpath differs)");
        if (reflection->m_prefix != m_prefix) throw chronology_error("invalid reflection (prefix differs)");
        flush();
        m_reflection = reflection;
        m_reflection->m_reg.adopt(m_reg);
        m_reflection->load();
        assert(!m_reflection->m_file || m_reflection->m_file->readonly());
    }

    bool operator==(const chronology& other) const {
        if (m_current_time != other.m_current_time) return false;
        if (m_dictionary.size() != other.m_dictionary.size()) return false;
        if (m_references.size() != other.m_references.size()) return false;
        {
            chv_stream reg1, reg2;
            reg1 << m_reg; reg2 << other.m_reg;
            const std::string s1 = reg1.to_string();
            const std::string s2 = reg2.to_string();
            if (s1 != s2) return false;
        }
        {
            auto kv1 = m_dictionary.begin();        auto kv2 = other.m_dictionary.begin();
            const auto& end1 = m_dictionary.end();  const auto& end2 = other.m_dictionary.end();
            while (kv1 != end1 && kv2 != end2) {
                if (kv1->first != kv2->first) return false;
                if (*(kv1->second) != *(kv2->second)) return false;
                ++kv1; ++kv2;
            }
        }
        {
            auto kv1 = m_references.begin();        auto kv2 = other.m_references.begin();
            const auto& end1 = m_references.end();  const auto& end2 = other.m_references.end();
            while (kv1 != end1 && kv2 != end2) {
                if (kv1->first != kv2->first) return false;
                if (kv1->second != kv2->second) return false;
                ++kv1; ++kv2;
            }
        }
        return true;
    }

    inline bool operator!=(const chronology& other) const { return !operator==(other); }

    void period() {
        if (!m_reflection) return;
        assert(!m_reflection->m_file || m_reflection->m_file->readonly());
        flush();
        // fread will not realize there is more data in a file so we reopen
        m_reflection->m_file->reopen();
        while (m_reflection->registry_iterate(m_reflection->m_file));
        if (*this != *m_reflection) {
            operator==(*m_reflection);
            throw chronology_error("reflection check failed");
        }
    }
#endif // USE_REFLECTION

    virtual void compress(serializer* stm, const std::vector<H>& references) override {
        assert(stm == m_file);
        // generate known bit field
        size_t refs = references.size();
        bitfield bf(refs);
        for (size_t i = 0; i < refs; ++i) if (m_references.count(references[i])) bf.set(i); else bf.unset(i);
        // length of vector as varint
        *m_file << varint(refs);
        // write bitfield
        *m_file << bf;
        for (size_t i = 0; i < refs; ++i) {
            if (bf[i]) {
                *m_file << varint(m_file->tell() - m_references.at(references[i]));
            } else {
                serialize(*m_file, references[i]);
            }
        }
    }

    virtual void compress(serializer* stm, const H& reference) override {
        assert(stm == m_file);
        uint8_t known = m_references.count(reference);
        *m_file << known;
        if (known) {
            *m_file << varint(m_file->tell() - m_references.at(reference));
        } else {
            serialize(*m_file, reference);
        }
    }

    virtual void decompress(serializer* stm, std::vector<H>& references) override {
        assert(stm == m_file);
        // length of vector as varint
        size_t refs = varint::load(m_file);
        // fetch known bit field
        bitfield bf(refs);
        *m_file >> bf;
        H u;
        for (size_t i = 0; i < refs; ++i) {
            if (bf[i]) {
                references.push_back(m_dictionary.at(m_file->tell() - varint::load(m_file))->m_hash);
            } else {
                deserialize(*stm, u);
                references.push_back(u);
            }
        }
    }

    virtual void decompress(serializer* stm, H& reference) override {
        assert(stm == m_file);
        uint8_t known;
        *m_file >> known;
        if (known) {
            reference = m_dictionary.at(m_file->tell() - varint::load(m_file))->m_hash;
        } else {
            deserialize(*m_file, reference);
        }
    }

    inline std::shared_ptr<T> tretch(const H& hash) { return m_references.count(hash) ? m_dictionary.at(m_references.at(hash)) : nullptr; }

    chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024, bool readonly = false)
    :   m_current_time(0)
#ifdef USE_REFLECTION
    ,   m_reflection(nullptr)
#endif // USE_REFLECTION
    ,   db<H>(dbpath, prefix, cluster_size, readonly)
    {}

    //////////////////////////////////////////////////////////////////////////////////////
    // Writing
    //

    void push_event(long timestamp, uint8_t cmd, std::shared_ptr<T> subject = nullptr, bool refer_only = true) {
        if (!m_file) throw db_error("event pushed with null sector (begin sector(s) first)");
        assert(timestamp >= m_current_time);
        bool known = subject.get() && m_references.count(subject->m_hash);
        uint8_t header_byte = cmd | (known << 5) | time_rel_bits(timestamp - m_current_time);
        *m_file << header_byte;
        _write_time(H, header_byte, m_current_time, timestamp); // this updates m_current_time
        if (subject.get()) {
            if (known) {
                refer(subject.get());
            } else if (refer_only) {
                refer(subject->m_hash);
            } else {
                id obid = db<H>::store(subject.get());
                m_dictionary[obid] = subject;
                m_references[subject->m_hash] = obid;
            }
        }
    }

    void push_event(long timestamp, uint8_t cmd, const std::set<std::shared_ptr<T>>& subjects) {
        push_event(timestamp, cmd);
        object<H>* ts[subjects.size()];
        size_t i = 0;
        for (auto& tp : subjects) {
            ts[i++] = tp.get();
        }
        refer(ts, i);
    }

    void push_event(long timestamp, uint8_t cmd, const std::set<H>& subject_hashes) {
        push_event(timestamp, cmd);
        std::set<std::shared_ptr<T>> pool;
        object<H>* ts[subject_hashes.size()];
        size_t i = 0;
        for (auto& hash : subject_hashes) {
            if (m_references.count(hash)) {
                // known
                ts[i] = m_dictionary.at(m_references.at(hash)).get();
            } else {
                // unknown
                auto ob = std::make_shared<T>(this, hash);
                pool.insert(ob);
                ts[i] = ob.get();
            }
            ++i;
        }
        refer(ts, i);
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // Reading
    //

    bool _pop_next(uint8_t& cmd, bool& known, long& time, bool peeking = false) {
        uint8_t u8, timerel;
        while (m_file->readonly() && m_file->eof()) {
            auto next_cluster = m_reg.cluster_next(m_reg.m_current_cluster);
            if (next_cluster == nullid) return false;
            m_ic.open(next_cluster, true);
        }
        auto pos = m_ic.m_file->tell();
        try {
            read_cmd_time(H, u8, cmd, known, timerel, time, m_current_time);
            if (peeking) m_ic.m_file->seek(pos, SEEK_SET);
        } catch (io_error) {
            return false;
        } catch (std::ios_base::failure& f) {
            return false;
        }
        return true;
    }

    bool peek_time(long& time) {
        uint8_t cmd;
        bool known;
        return _pop_next(cmd, known, time, true);
    }

    bool pop_event(uint8_t& cmd, bool& known) {
        return _pop_next(cmd, known, m_current_time);
    }

    std::shared_ptr<T> pop_object() {
        std::shared_ptr<T> object = std::make_shared<T>(this);
        db<H>::load(object.get());
        id obid = object->m_sid;
        m_dictionary[obid] = object;
        m_references[object->m_hash] = obid;
        return m_dictionary.at(obid);
    }

    id pop_reference()        { return derefer(); }
    H& pop_reference(H& hash) { return derefer(hash); }

    void pop_references(std::set<id>& known, std::set<H>& unknown) {
        known.clear();
        unknown.clear();
        derefer(known, unknown);
    }

    void pop_reference_hashes(std::set<H>& mixed) {
        std::set<id> known;
        pop_references(known, mixed);
        for (id i : known) {
            if (!m_dictionary.count(i)) fprintf(stderr, "*** pop_reference_hashes(): unknown key %llu\n", i);
            mixed.insert(m_dictionary.at(i)->m_hash);
        }
    }

    // virtual void registry_opened_cluster(id cluster, file* file) override {
    //     db<H>::registry_opened_cluster(cluster, file);
    //     file->m_compressor = this;
    // }

    virtual void registry_closing_cluster(id cluster) override {
        db<H>::registry_closing_cluster(cluster);
        for (auto& kv : m_dictionary) kv.second->m_sid = unknownid;
        m_dictionary.clear();
        m_references.clear();
        m_current_time = 0;
    }

    virtual void goto_segment(id segment_id) override {
        if (m_reg.prepare_cluster_for_segment(segment_id) != m_reg.m_current_cluster) {
            // TODO: the time will be wrong when jumping to segments not evenly divisible with cluster size
            m_current_time = 0;
        }
        db<H>::goto_segment(segment_id);
    }

    virtual void begin_segment(id segment_id) override {
        if (m_reg.prepare_cluster_for_segment(segment_id) != m_reg.m_current_cluster) {
            m_current_time = 0;
        }
        db<H>::begin_segment(segment_id);
#ifdef USE_REFLECTION
        if (m_reflection) {
            flush();
            m_reflection->begin_segment(segment_id);
        }
#endif // USE_REFLECTION
    }
};

// db

template<typename H> db<H>::db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size, bool readonly)
    : m_dbpath(dbpath)
    , m_prefix(prefix)
    , m_reg(this, dbpath, prefix, cluster_size)
    , m_file(nullptr)
    , m_ic(&m_reg, readonly)
    , m_readonly(readonly)
{
    if (!mkdir(m_dbpath)) {
        try {
            file regfile(m_dbpath + "/cq.registry", true);
            regfile >> m_reg;
        } catch (const fs_error& err) {
            // we do not catch io_error's, and an io_error is thrown if cq.registry existed but deserialization failed,
            // which should be a crash
        }
    }
}

template<typename H> void db<H>::open(id cluster, bool readonly) {
    if (!readonly && m_readonly) throw db_error("readonly database");
    m_ic.open(cluster, readonly);
}

template<typename H> void db<H>::load() {
    m_ic.resume(false);
}

template<typename H> db<H>::~db() {
    if (!m_readonly) {
        file regfile(m_dbpath + "/cq.registry", false, true);
        regfile << m_reg;
    }
    m_ic.close();
}

template<typename H> void db<H>::registry_closing_cluster(id cluster) {}

template<typename H> void db<H>::registry_opened_cluster(id cluster, file* file) {
    m_file = file;
    if (m_readonly) assert(m_file->readonly());
}

//
// db registry
//

template<typename H> id db<H>::store(object<H>* t) {
    if (!m_file) throw db_error("invalid operation -- db not ready (no segment begun)");
    if (m_readonly) throw db_error("readonly database");
    if (m_file->readonly()) throw db_error("file is readonly");
    assert(t);
    id rval = m_file->tell();
    *m_file << *t;
    t->m_sid = rval;
    return rval;
}

template<typename H> void db<H>::load(object<H>* t) {
    assert(m_file);
    assert(t);
    id rval = m_file->tell();
    *m_file >> *t;
    t->m_sid = rval;
}

template<typename H> void db<H>::fetch(object<H>* t, id i) {
    assert(m_file);
    assert(t);
    long p = m_file->tell();
    if (p != i) m_file->seek(i, SEEK_SET);
    *m_file >> *t;
    if (p != m_file->tell()) m_file->seek(p, SEEK_SET);
    t->m_sid = i;
}

template<typename H> void db<H>::refer(id sid) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    assert(sid < m_file->tell());
    *m_file << varint(m_file->tell() - sid);
}

template<typename H> void db<H>::refer(object<H>* t) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    assert(t);
    assert(t->m_sid != unknownid);
    assert(t->m_sid < m_file->tell());
    *m_file << varint(m_file->tell() - t->m_sid);
}

template<typename H> void db<H>::refer(const H& hash) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    serialize(*m_file, hash);
}

template<typename H> id db<H>::derefer() {
    assert(m_file);
    return m_file->tell() - varint::load(m_file);
}

template<typename H> H& db<H>::derefer(H& hash) {
    assert(m_file);
    deserialize(*m_file, hash);
    return hash;
}

template<typename H> void db<H>::refer(object<H>** ts, size_t sz) {
    if (m_readonly) throw db_error("readonly database");
    id known, unknown;
    assert(ts);
    assert(sz < 65536);

    known = 0;
    size_t klist[sz];
    size_t idx = 0;
    for (size_t i = 0; i < sz; ++i) {
        if (ts[i]->m_sid) {
            known++;
            klist[idx++] = i;
        }
    }
    unknown = sz - known;

    // bits:    purpose:
    // 0-3      1111 = known is 15 + a varint starting at next (available) byte, 0000~1110 = there are byte(bits) known (0-14)
    // 4-7      ^ s/known/unknown/g

    cond_varint<4> known_vi(known);
    cond_varint<4> unknown_vi(unknown);

    uint8_t multi_refer_header =
        (  known_vi.byteval()     )
    |   (unknown_vi.byteval() << 4);

    *m_file << multi_refer_header;
    known_vi.cond_serialize(m_file);
    unknown_vi.cond_serialize(m_file);

    // write known objects
    // TODO: binomial encoding etc
    id refpoint = m_file->tell();
    for (id i = 0; i < known; ++i) {
        *m_file << varint(refpoint - ts[klist[i]]->m_sid);
    }
    // write unknown object refs
    for (id i = 0; i < sz; ++i) {
        if (ts[i]->m_sid == unknownid) {
            serialize(*m_file, ts[i]->m_hash);
        }
    }
}

template<typename H> void db<H>::derefer(std::set<id>& known_out,  std::set<H>& unknown_out) {
    known_out.clear();
    unknown_out.clear();

    uint8_t multi_refer_header;
    *m_file >> multi_refer_header;
    cond_varint<4> known_vi(multi_refer_header & 0x0f, m_file);
    cond_varint<4> unknown_vi(multi_refer_header >> 4, m_file);
    id known = known_vi.m_value;
    id unknown = unknown_vi.m_value;

    // read known objects
    // TODO: binomial encoding etc
    id refpoint = m_file->tell();
    for (id i = 0; i < known; ++i) {
        known_out.insert(refpoint - varint::load(m_file));
    }
    // read unknown refs
    for (id i = 0; i < unknown; ++i) {
        H h;
        deserialize(*m_file, h);
        unknown_out.insert(h);
    }
}

template<typename H> void db<H>::begin_segment(id segment_id) {
    if (segment_id < m_reg.m_tip) throw db_error("may not begin a segment < current tip");
    id new_cluster = m_reg.prepare_cluster_for_segment(segment_id);
    assert(m_reg.m_tip == segment_id || !m_file);
    bool write_reg = false;
    if (new_cluster != m_reg.m_current_cluster || !m_file) {
        write_reg = !m_readonly;
        m_ic.open(new_cluster, m_readonly);
    }
    m_reg.m_forward_index.mark_segment(segment_id, m_file->tell());
    if (write_reg) {
        file regfile(m_dbpath + "/cq.registry", false, true);
        regfile << m_reg;
    }
}

template<typename H> void db<H>::goto_segment(id segment_id) {
    id new_cluster = m_reg.prepare_cluster_for_segment(segment_id);
    if (new_cluster != m_reg.m_current_cluster || !m_file) {
        m_ic.open(new_cluster, true);
    }
    if (segment_id == 0 && m_reg.m_forward_index.get_segment_count() == 0) {
        // empty inital cluster; stop here and let iteration deal
        return;
    }
    id pos = 0;
    if (m_reg.m_forward_index.has_segment(segment_id)) {
        pos = m_reg.m_forward_index.get_segment_position(segment_id);
    } else if (m_reg.m_forward_index.get_segment_count() > 0) {
        pos = m_reg.m_forward_index.get_segment_position(m_reg.m_forward_index.get_first_segment());
    }
    m_file->seek(pos, SEEK_SET);
}

template<typename H> void db<H>::flush() {
    if (m_readonly) throw db_error("readonly database");
    assert(m_ic.m_file == m_file);
    m_ic.flush();
    // if (m_file) m_file->flush();
}

} // namespace cq

#endif // included_cq_cq_h_
