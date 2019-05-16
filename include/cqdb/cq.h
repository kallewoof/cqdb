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

struct registry : serializable {
    /**
     * A list of existing clusters in the registry.
     */
    unordered_set m_clusters;
    uint32_t m_cluster_size;
    id m_tip;
    prepare_for_serialization();
    inline id open_cluster_for_segment(id segment) {
        if (segment > m_tip) {
            if (m_clusters.m.size() == 0 || segment / m_cluster_size > m_tip / m_cluster_size) {
                m_clusters.m.insert(segment / m_cluster_size);
            }
            m_tip = segment;
        }
        return segment / m_cluster_size;
    }
    registry(uint32_t cluster_size = 1024) : m_cluster_size(cluster_size), m_tip(0) {}
    inline bool operator==(const registry& other) const { return m_cluster_size == other.m_cluster_size && m_clusters == other.m_clusters; }
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
    serializer* open(const std::string& fname, bool readonly);
    serializer* open(id fref, bool readonly);

    void close();

public:
    serializer* m_file;

    // struction
    db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024);
    ~db();

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

#define time_rel_value(cmd) (((cmd) >> 6) & 0x3)
inline uint8_t time_rel_bits(int64_t time) { return ((time < 3 ? time : 3) << 6); }

#define _read_time(t, current_time, timerel) \
    if (timerel < 3) { \
        t = current_time + timerel; \
    } else { \
        t = current_time + varint::load(m_file); \
    }

#define read_cmd_time(u8, cmd, known, timerel, time) do { \
        u8 = m_file->get_uint8(); \
        cmd = (u8 & 0x1f); /* 0b0001 1111 */ \
        known = 0 != (u8 & 0x20); \
        timerel = time_rel_value(u8); \
        _read_time(time, time, timerel); \
    } while(0)

#define _write_time(rel, current_time, write_time) do { \
        if (time_rel_value(rel) > 2) { \
            uint64_t tfull = uint64_t(write_time - current_time); \
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
public:
    long current_time;
    std::map<id, std::shared_ptr<T>> m_dictionary;
    std::map<uint256, id> m_references;

    chronology(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size = 1024)
    : current_time(0)
    , db(dbpath, prefix, cluster_size)
    {}

    //////////////////////////////////////////////////////////////////////////////////////
    // Writing
    //

    void push_event(long timestamp, uint8_t cmd, std::shared_ptr<T> subject = nullptr, bool refer_only = true) {
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
        } catch (std::ios_base::failure& f) {
            return false;
        }
        return true;
    }

    std::shared_ptr<T>& pop_object(std::shared_ptr<T>& object) { load(object.get()); return object; }
    id pop_reference()                                         { return derefer(); }
    uint256& pop_reference(uint256& hash)                      { return derefer(hash); }

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

} // namespace cq

#endif // included_cq_cq_h_
