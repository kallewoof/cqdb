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

class object : public serializable {
public:
    id m_sid;
    uint256 m_hash;
    explicit object(const uint256& hash) : object(0, hash) {}
    object(id sid = 0, const uint256& hash = uint256()) : m_sid(sid), m_hash(hash) {}
    bool operator==(const object& other) const {
        return m_sid == other.m_sid && m_hash == other.m_hash;
    }
};

class registry : serializable {
private:
    /**
     * A list of existing clusters in the registry.
     */
    unordered_set m_clusters;
public:
    uint32_t m_cluster_size;
    id m_tip;
    prepare_for_serialization();
    id open_cluster_for_segment(id segment);
    registry(uint32_t cluster_size = 1024) : m_cluster_size(cluster_size), m_tip(0) {}
    inline bool operator==(const registry& other) const { return m_cluster_size == other.m_cluster_size && m_clusters == other.m_clusters && m_tip == other.m_tip; }
    inline const unordered_set& get_clusters() const { return m_clusters; }
};

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
    prepare_for_serialization();
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

typedef class header footer;

class db {
protected:
    const std::string m_dbpath;
    const std::string m_prefix;
    id m_cluster;
    registry m_reg;
    header* m_header; // this is the worked-on header for the current (unfinished) cluster
    footer* m_footer; // this is the readonly footer referencing the previous cluster, if any
    bool m_readonly;

    std::string cluster_path(id cluster);
    void open(bool readonly);
    void resume();
    virtual void prepare_for_writing() {
        // seek to end
        m_file->seek(0, SEEK_END);
    }
    serializer* open(const std::string& fname, bool readonly);
    serializer* open(id fref, bool readonly);

    void close();

public:
    serializer* m_file;

    // struction
    db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024);
    virtual ~db();
    void load();

    // registry
    id store(object* t);                    // writes object to disk and returns its absolute id
    void load(object* t);                   // reads object from disk at current position
    void fetch(object* t, id i);            // fetches object with obid 'i' into *t from disk
    void refer(object* t);                  // writes a reference to t to disk (known)
    id derefer();                           // reads a reference id from disk
    void refer(const uint256& hash);        // write a reference to an unknown object to disk
    uint256& derefer(uint256& hash);        // reads a reference to an unknown object from disk

    void refer(object** ts, size_t sz);     // writes an unordered set of references to sz number of objects
    void derefer(std::set<id>& known        // reads an unordered set of known/unknown references from disk
               , std::set<uint256>& unknown);

    inline const registry& get_registry() const { return m_reg; }
    inline const id& get_cluster() const { return m_cluster; }
    inline const header* get_header() const { return m_header; }
    inline const footer* get_footer() const { return m_footer; }
    inline std::string stell() const { char v[256]; sprintf(v, "%lld:%ld", m_cluster, m_file ? m_file->tell() : -1); return v; }

    /**
     * Segments are important positions in the stream of events which are referencable
     * from the follow-up header. Segments must be strictly increasing, but may include
     * gaps. This will automatically jump to the {last file, end of file} position and
     * disable `readonly` flag, (re-)enabling all write operations.
     */
    void begin_segment(id segment_id);

    /**
     * Seek to the {file, position} for the given segment.
     * Enables `readonly` flag, disabling all write operations.
     */
    void goto_segment(id segment_id);

    /**
     * Notification that a cluster change occurred. Changing clusters means that
     * all "known" objects must be forgotten.
     * In the future, this should be intelligent about not dropping everything, but
     * to only drop items referenced from footer, but this is not implemented yet.
     */
    virtual void cluster_changed(id old_cluster_id, id new_cluster_id) {}

    void flush();
};

inline uint8_t time_rel_value(uint8_t cmd) { return cmd >> 6; }
inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

#define _read_time(t, current_time, timerel) \
    t = current_time + timerel + (timerel > 2 ? varint::load(m_file) : 0)

#define read_cmd_time(u8, cmd, known, timerel, time) do { \
        u8 = m_file->get_uint8(); \
        cmd = (u8 & 0x1f); /* 0b0001 1111 */ \
        known = 0 != (u8 & 0x20); \
        timerel = time_rel_value(u8); \
        _read_time(time, time, timerel); \
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
class chronology : public db {
protected:
    void prepare_for_writing() override {
        // we need to fetch all objects that we are meant to know about
        // the only way to do this by default is to replay the file up to the end
        while (iterate());
    }

public:
    long current_time;
    std::map<id, std::shared_ptr<T>> m_dictionary;
    std::map<uint256, id> m_references;

    chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    : current_time(0)
    , db(dbpath, prefix, cluster_size)
    {}

    virtual bool iterate() =0;

    //////////////////////////////////////////////////////////////////////////////////////
    // Writing
    //

    void push_event(long timestamp, uint8_t cmd, std::shared_ptr<T> subject = nullptr, bool refer_only = true) {
        assert(timestamp >= current_time);
        bool known = subject.get() && m_references.count(subject->m_hash);
        uint8_t header_byte = cmd | (known << 5) | time_rel_bits(timestamp - current_time);
        *m_file << header_byte;
        _write_time(header_byte, current_time, timestamp); // this updates current_time
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

    bool pop_event(uint8_t& cmd, bool& known) {
        uint8_t u8, timerel;
        try {
            read_cmd_time(u8, cmd, known, timerel, current_time);
        } catch (io_error) {
            return false;
        } catch (std::ios_base::failure& f) {
            return false;
        }
        return true;
    }

    std::shared_ptr<T> pop_object() {
        std::shared_ptr<T> object = std::make_shared<T>();
        auto pos = m_file->tell();
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
            mixed.insert(m_dictionary.at(i)->m_hash);
        }
    }

    virtual void cluster_changed(id old_cluster_id, id new_cluster_id) override {
        m_dictionary.clear();
        m_references.clear();
    }
};

} // namespace cq

#endif // included_cq_cq_h_
