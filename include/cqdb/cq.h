#ifndef included_cq_cq_h_
#define included_cq_cq_h_

#include <string>
#include <map>
#include <memory>
#include <ios>

#include <cstdlib>
#include <cstdio>

#include <cqdb/io.h>

#include <cqdb/uint256.h>

namespace cq {

class db_error : public std::runtime_error { public: explicit db_error(const std::string& str) : std::runtime_error(str) {} };
class chronology_error : public std::runtime_error { public: explicit chronology_error(const std::string& str) : std::runtime_error(str) {} };

#define OBREF(known, x) if (x.get()) { if (known) { refer(x.get()); } else { refer(x->m_hash); } }
#define FERBO(known, u256) known ? m_dictionary.at(pop_reference())->m_hash : pop_reference(u256)

constexpr id unknownid = 0x0;

class object : public serializable {
public:
    id m_sid;
    uint256 m_hash;
    explicit object(const uint256& hash) : object(unknownid, hash) {}
    object(id sid = unknownid, const uint256& hash = uint256()) : m_sid(sid), m_hash(hash) {}
    bool operator==(const object& other) const {
        return m_hash == other.m_hash;
    }
    inline bool operator!=(const object& other) const { return !operator==(other); }
};

static const uint8_t HEADER_VERSION = 1;

class header : public serializable {
private:
    uint8_t m_version;                  //!< CQ version byte
    uint64_t m_timestamp_start;         //!< internal representation starting timestamp (e.g. median time of first bitcoin block in list)
    /**
     * A map of segment ID : file position, where the segment ID in bitcoin's case refers to the block height.
     */
    incmap m_segments;
public:
    id m_cluster;

    header(uint8_t version, uint64_t timestamp, id cluster);
    header(id cluster, serializer* stream);
    void reset(uint8_t version, uint64_t timestamp, id cluster);

    prepare_for_serialization();

    void adopt(const header& other) {
        assert(m_version == other.m_version);
        m_timestamp_start = other.m_timestamp_start;
        m_segments = other.m_segments;
        m_cluster = other.m_cluster;
    }

    void mark_segment(id segment, id position);
    id get_segment_position(id segment) const;
    id get_first_segment() const;
    id get_last_segment() const;
    size_t get_segment_count() const;
    uint8_t get_version() const { return m_version; }
    uint64_t get_timestamp_start() const { return m_timestamp_start; }
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
    ,   m_forward_index(HEADER_VERSION, 0, nullid)
    ,   m_back_index(HEADER_VERSION, 0, nullid)
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

class db : public registry_delegate {
protected:
    const std::string m_dbpath;
    const std::string m_prefix;
    registry m_reg;

    void open(id cluster, bool readonly);
    void reopen();

    void close();
    bool m_readonly;

public:
    file* m_file;
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
    id store(object* t);                    // writes object to disk and returns its absolute id
    void load(object* t);                   // reads object from disk at current position
    void fetch(object* t, id i);            // fetches object with obid 'i' into *t from disk
    void refer(id sid);                     // writes a reference to the object with the given sid
    void refer(object* t);                  // writes a reference to t to disk (known)
    id derefer();                           // reads a reference id from disk
    void refer(const uint256& hash);        // write a reference to an unknown object to disk
    uint256& derefer(uint256& hash);        // reads a reference to an unknown object from disk

    void refer(object** ts, size_t sz);     // writes an unordered set of references to sz number of objects
    void derefer(std::set<id>& known        // reads an unordered set of known/unknown references from disk
               , std::set<uint256>& unknown);

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

    void flush();
};

inline uint8_t time_rel_value(uint8_t cmd) { return cmd >> 6; }
inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

#define _read_time(t, current_time, timerel) \
    t = current_time + timerel + (timerel > 2 ? varint::load(m_file) : 0)

#define read_cmd_time(u8, cmd, known, timerel, time, current_time) do { \
        u8 = m_file->get_uint8(); \
        cmd = (u8 & 0x1f); /* 0b0001 1111 */ \
        known = 0 != (u8 & 0x20); \
        timerel = time_rel_value(u8); \
        _read_time(time, current_time, timerel); \
    } while(0)

#define _write_time(rel, current_time, write_time) do { \
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
template<typename T>
class chronology : public db, public compressor {
public:
    long m_current_time;
    std::map<id, std::shared_ptr<T>> m_dictionary;
    std::map<uint256, id> m_references;
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

    virtual void compress(serializer* stm, const std::vector<uint256>& references) override {
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
                references[i].Serialize(*m_file);
            }
        }
    }

    virtual void compress(serializer* stm, const uint256& reference) override {
        assert(stm == m_file);
        uint8_t known = m_references.count(reference);
        *m_file << known;
        if (known) {
            *m_file << varint(m_file->tell() - m_references.at(reference));
        } else {
            reference.Serialize(*m_file);
        }
    }

    virtual void decompress(serializer* stm, std::vector<uint256>& references) override {
        assert(stm == m_file);
        // length of vector as varint
        size_t refs = varint::load(m_file);
        // fetch known bit field
        bitfield bf(refs);
        *m_file >> bf;
        uint256 u;
        for (size_t i = 0; i < refs; ++i) {
            if (bf[i]) {
                references.push_back(m_dictionary.at(m_file->tell() - varint::load(m_file))->m_hash);
            } else {
                u.Unserialize(*stm);
                references.push_back(u);
            }
        }
    }

    virtual void decompress(serializer* stm, uint256& reference) override {
        assert(stm == m_file);
        uint8_t known;
        *m_file >> known;
        if (known) {
            reference = m_dictionary.at(m_file->tell() - varint::load(m_file))->m_hash;
        } else {
            reference.Unserialize(*m_file);
        }
    }

    inline std::shared_ptr<T> tretch(const uint256& hash) { return m_references.count(hash) ? m_dictionary.at(m_references.at(hash)) : nullptr; }

    chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024, bool readonly = false)
    :   m_current_time(0)
    ,   m_reflection(nullptr)
    ,   db(dbpath, prefix, cluster_size, readonly)
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
        _write_time(header_byte, m_current_time, timestamp); // this updates m_current_time
        if (subject.get()) {
            if (known) {
                refer(subject.get());
            } else if (refer_only) {
                refer(subject->m_hash);
            } else {
                id obid = store(subject.get());
                m_dictionary[obid] = subject;
                m_references[subject->m_hash] = obid;
            }
        }
    }

    void push_event(long timestamp, uint8_t cmd, const std::set<std::shared_ptr<T>>& subjects) {
        push_event(timestamp, cmd);
        object* ts[subjects.size()];
        size_t i = 0;
        for (auto& tp : subjects) {
            ts[i++] = tp.get();
        }
        refer(ts, i);
    }

    void push_event(long timestamp, uint8_t cmd, const std::set<uint256>& subject_hashes) {
        push_event(timestamp, cmd);
        std::set<std::shared_ptr<T>> pool;
        object* ts[subject_hashes.size()];
        size_t i = 0;
        for (auto& hash : subject_hashes) {
            if (m_references.count(hash)) {
                // known
                ts[i] = m_dictionary.at(m_references.at(hash)).get();
            } else {
                // unknown
                auto ob = std::make_shared<T>(hash);
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
            read_cmd_time(u8, cmd, known, timerel, time, m_current_time);
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
        std::shared_ptr<T> object = std::make_shared<T>();
        load(object.get());
        id obid = object->m_sid;
        m_dictionary[obid] = object;
        m_references[object->m_hash] = obid;
        return m_dictionary.at(obid);
    }

    id pop_reference()                    { return derefer(); }
    uint256& pop_reference(uint256& hash) { return derefer(hash); }

    void pop_references(std::set<id>& known, std::set<uint256>& unknown) {
        known.clear();
        unknown.clear();
        derefer(known, unknown);
    }

    void pop_reference_hashes(std::set<uint256>& mixed) {
        std::set<id> known;
        pop_references(known, mixed);
        for (id i : known) {
            if (!m_dictionary.count(i)) fprintf(stderr, "*** pop_reference_hashes(): unknown key %llu\n", i);
            mixed.insert(m_dictionary.at(i)->m_hash);
        }
    }

    virtual void registry_opened_cluster(id cluster, file* file) override {
        db::registry_opened_cluster(cluster, file);
        file->m_compressor = this;
    }

    virtual void registry_closing_cluster(id cluster) override {
        db::registry_closing_cluster(cluster);
        for (auto& kv : m_dictionary) kv.second->m_sid = unknownid;
        m_dictionary.clear();
        m_references.clear();
    }

    virtual void begin_segment(id segment_id) override {
        db::begin_segment(segment_id);
        if (m_reflection) {
            // must be manually updated as the forward index is assumed to not change from under you
            m_reflection->m_reg.m_forward_index.mark_segment(segment_id, m_file/* yes, m_file! */->tell());
            if (m_reflection->m_reg.m_tip < segment_id) m_reflection->m_reg.m_tip = segment_id;
        }
    }
};

} // namespace cq

#endif // included_cq_cq_h_
