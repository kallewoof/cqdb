#ifndef included_cq_io_h_
#define included_cq_io_h_

#include <string>
#include <map>
#include <set>
#include <vector>

#include <cstdlib>
#include <cstdio>
#include <inttypes.h> // PRIu64
#include <cstring>

#ifdef _WIN32
#   include <direct.h>
#   include <windows.h>
#   include <tchar.h>
#else
#   include <dirent.h>
#endif

namespace cq {

class fs_error : public std::runtime_error { public: explicit fs_error(const std::string& str) : std::runtime_error(str) {} };
class io_error : public std::runtime_error { public: explicit io_error(const std::string& str) : std::runtime_error(str) {} };

bool mkdir(const std::string& path);
bool rmdir(const std::string& path);
bool rmfile(const std::string& path);
bool listdir(const std::string& path, std::vector<std::string>& list);
bool rmdir_r(const std::string& path);
void randomize(void* dst, size_t bytes);
long fsize(const std::string& path);

typedef uint64_t id;
constexpr id nullid = 0xffffffffffffffff;
#define PRIid PRIu64

template<typename T, typename Stream> void serialize(Stream& stm, const std::vector<T>& vec);
template<typename T, typename Stream> void deserialize(Stream& stm, std::vector<T>& vec);

#define S(T) \
    template<typename Stream> void serialize(Stream& stm, T t) { stm.w(t); } \
    template<typename Stream> void deserialize(Stream& stm, T& t) { stm.r(t); }
S(uint8_t); S(uint16_t); S(uint32_t); S(uint64_t); S(int8_t); S(int16_t); S(int32_t); S(int64_t);
#undef S

class serializer {
public:
    virtual ~serializer() {}
    virtual bool eof() { return true; /* streams are by default always eof */ }
    virtual bool empty() { return tell() == 0 && eof(); }
    virtual size_t write(const uint8_t* data, size_t len) { throw io_error("write-only stream"); /* override to make writeable */ }
    virtual size_t read(uint8_t* data, size_t len) { throw io_error("readonly stream"); /* override to make readable */ }
    virtual void seek(long offset, int whence) { throw io_error("non-seekable stream"); /* override to make seekable */ }
    virtual long tell()  { throw io_error("stream without tell support"); /* override to make tellable */ }
    uint8_t get_uint8();
    virtual void flush() {}

    // bitcoin core compatibility
    inline size_t write(const char* data, size_t len) { return write((const uint8_t*)data, len); }
    inline size_t read(char* data, size_t len) { return read((uint8_t*)data, len); }
    virtual int GetVersion() const { return 0; }

    template<typename T> inline size_t w(const T& data) { return write((const uint8_t*)&data, sizeof(T)); }
    template<typename T> inline size_t r(T& data)        { return read((uint8_t*)&data, sizeof(T)); }

    template<typename T> serializer& operator<<(const T& obj) { serialize(*this, obj); return *this; }
    template<typename T> serializer& operator>>(T& obj) { deserialize(*this, obj); return *this; }

    virtual std::string to_string() const { return "?"; }
};

class serializable {
public:
    virtual void serialize(serializer* stream) const =0;
    virtual void deserialize(serializer* stream) =0;
};

struct sizer : public serializer {
    size_t m_len{0};
    sizer() {}
    sizer(serializable* s) {
        s->serialize(this);
    }
    size_t write(const uint8_t* data, size_t len) override { m_len += len; return len; }
    size_t read(uint8_t* data, size_t len) override { m_len += len; return len; }
    bool eof() override { return false; }
    void seek(long offset, int whence) override {}
    long tell() override { return (long)m_len; }
};

#define prepare_for_serialization() \
    virtual void serialize(::cq::serializer* stream) const override; \
    virtual void deserialize(::cq::serializer* stream) override

struct varint : public serializable {
    id m_value;
    explicit varint(id value = 0) : m_value(value) {}
    explicit varint(serializer* s) { deserialize(s); }
    prepare_for_serialization();
    static inline id load(serializer* s) { varint v(s); return v.m_value; }
};

template<typename Stream> void serialize(Stream& stm, const std::string& str) {
    varint(str.size()).serialize(&stm);
    stm.write((const uint8_t*)str.data(), str.size());
}

template<typename Stream> void deserialize(Stream& stm, std::string& str) {
    size_t sz = varint::load(&stm);
    str.resize(sz);
    stm.read((uint8_t*)str.data(), sz);
}

template<typename T, typename Stream> void serialize(Stream& stm, const std::vector<T>& vec) {
    varint(vec.size()).serialize(&stm);
    for (const T& v : vec) serialize(stm, v);
}

template<typename T, typename Stream> void deserialize(Stream& stm, std::vector<T>& vec) {
    vec.resize(varint::load(&stm));
    for (size_t i = 0; i < vec.size(); ++i) deserialize(stm, vec[i]);
}

template<typename Stream> inline void serialize(Stream& stm, const serializable* ob) { ob->serialize(stm); }
template<typename Stream> inline void deserialize(Stream& stm, serializable* ob)     { ob->deserialize(stm); }

template<typename T, typename Stream> inline void serialize(Stream& stm, const T& ob) { ob.serialize(&stm); }
template<typename T, typename Stream> inline void deserialize(Stream& stm, T& ob)     { ob.deserialize(&stm); }

struct conditional : public varint {
    using varint::varint;
    virtual ~conditional() {}
    virtual uint8_t byteval() const =0;
    virtual void cond_serialize(serializer* stream) const =0;
    virtual void cond_deserialize(uint8_t val, serializer* stream) =0;
};

template<uint8_t BITS>
struct cond_varint : public conditional {
private:
    // a 2 bit space would let us pre-describe the conditional varint as
    // 0b00 : 0
    // 0b01 : 1
    // 0b10 : 2
    // 0b11 : 3 + varint-value
    // so the cap here becomes (1 << 2) - 1 = 4 - 1 = 3 = 0b11
    // and anything BELOW the cap is pre-described completely, and at or above the cap requires an additional varint
    static constexpr uint8_t CAP = (1 << BITS) - 1;
public:
    using varint::m_value;
    using conditional::conditional;
    cond_varint(uint8_t val, serializer* s) {
        cond_deserialize(val, s);
    }
    uint8_t byteval() const override { return m_value < CAP ? m_value : CAP; }
    void cond_serialize(serializer* stream) const override {
        if (m_value >= CAP) {
            const_cast<cond_varint*>(this)->m_value -= CAP;
            varint::serialize(stream);
            const_cast<cond_varint*>(this)->m_value += CAP;
        }
    }
    void cond_deserialize(uint8_t val, serializer* stream) override {
        if (val < CAP) {
            m_value = val;
        } else {
            varint::deserialize(stream);
            m_value += CAP;
        }
    }
    void serialize(serializer* stream) const override {
        uint8_t val = m_value < CAP ? m_value : CAP;
        stream->w(val);
        cond_serialize(stream);
    }
    void deserialize(serializer* stream) override {
        uint8_t val;
        stream->r(val);
        cond_deserialize(val, stream);
    }
};

template<typename H> class compressor {
public:
    virtual void compress(serializer* stm, const std::vector<H>& references) { varint(references.size()).serialize(stm); for (const auto& u : references) serialize(*stm, u); }
    virtual void compress(serializer* stm, const H& reference) { serialize(*stm, reference); }
    virtual void decompress(serializer* stm, std::vector<H>& references) { id c = varint::load(stm); references.resize(c); for (id i = 0; i < c; ++i) deserialize(*stm, references[i]); }
    virtual void decompress(serializer* stm, H& reference) { deserialize(*stm, reference); }
};

/**
 * Incmaps are efficiently encoded maps linking two ordered sequences together. The two
 * sequences must be increasing s.t. each key and value can be expressed as the previous
 * key and value + positive integers (one for the key and one for the value).
 */
struct incmap : public serializable {
    std::map<id, id> m;
    prepare_for_serialization();
    bool operator==(const incmap& other) const;
    inline id at(id v) const { return m.at(v); }
    inline size_t count(id v) const { return m.count(v); }
    inline size_t size() const { return m.size(); }
    inline void clear() noexcept { m.clear(); }
};

struct unordered_set : public serializable {
    std::set<id> m;
    prepare_for_serialization();
    unordered_set() {}
    unordered_set(id* ids, size_t sz) { for (size_t i =0; i < sz; ++i) m.insert(ids[i]); }
    unordered_set(const std::set<id>& ids) { m.insert(ids.begin(), ids.end()); }
    bool operator==(const unordered_set& other) const { return m == other.m; }
    inline size_t size() const { return m.size(); }
    inline void clear() noexcept { m.clear(); }
};

class file : public serializer {
private:
    long m_tell;
    bool m_readonly;
    FILE* m_fp;
    std::string m_path;
public:
    file(FILE* fp);
    file(const std::string& path, bool readonly, bool clear = false);
    static bool accessible(const std::string& path);
    ~file() override;
    bool eof() override;
    using serializer::write;
    using serializer::read;
    size_t write(const uint8_t* data, size_t len) override;
    size_t read(uint8_t* data, size_t len) override;
    void seek(long offset, int whence) override;
    long tell() override;
    void flush() override { fflush(m_fp); }
    bool readonly() const { return m_readonly; }
    const std::string& get_path() const { return m_path; }
    void reopen();
};

class chv_stream : public serializer {
private:
    long m_tell{0};
    std::vector<uint8_t> m_chv;
public:
    bool eof() override;
    size_t write(const uint8_t* data, size_t len) override;
    size_t read(uint8_t* data, size_t len) override;
    void seek(long offset, int whence) override;
    long tell() override;
    void clear() { m_chv.clear(); m_tell = 0; }
    std::vector<uint8_t>& get_chv() { return m_chv; }
    std::string to_string() const override {
        char rv[(m_chv.size() << 1) + 1];
        char* rvp = rv;
        for (auto ch : m_chv) {
            rvp += sprintf(rvp, "%02x", ch);
        }
        return rv;
    }
};

class cluster_delegate {
public:
    virtual ~cluster_delegate() {}
    virtual id cluster_next(id cluster) =0;
    virtual id cluster_last(bool open_for_writing) =0;
    virtual std::string cluster_path(id cluster) =0;
    virtual void cluster_opened(id cluster, file* file) =0;
    virtual void cluster_will_close(id cluster) =0;
};

class cluster : public serializer {
public:
    id m_cluster{nullid};
    file* m_file;
    cluster_delegate* m_delegate;
    bool m_readonly;
    cluster(cluster_delegate* delegate, bool readonly);
    ~cluster() override;
    virtual void open(id cluster, bool readonly, bool clear = false);
    virtual void close() {}
    virtual void resume(bool clear = false); //!< iterate until the end of the cluster and, unless m_readonly is true, prepare to begin writing
    bool eof() override;
    size_t write(const uint8_t* data, size_t len) override;
    size_t read(uint8_t* data, size_t len) override;
    void seek(long offset, int whence) override;
    long tell() override;
    virtual void flush() override { m_file->flush(); }
};

class indexed_cluster_delegate : public cluster_delegate {
public:
    /**
     * Write the in-memory index for the current data block (`cluster`) being written
     * into the given serializer `file`. Repeat calls to this method may occur for the
     * same index.
     */
    virtual void cluster_write_forward_index(id cluster, file* file) =0;

    virtual void cluster_read_forward_index(id cluster, file* file) =0;

    virtual void cluster_clear_forward_index(id cluster) =0;

    /**
     * Read the back index from `file` for the data directly behind the data
     * block corresponding to the given id `cluster`. I.e. if cluster = 5, the
     * back index is the indexed content of cluster 4, stored in the header of
     * cluster 5. The data need not be retained.
     */
    virtual void cluster_read_back_index(id cluster, file* file) =0;

    virtual void cluster_clear_and_write_back_index(id cluster, file* file) =0;

    virtual bool cluster_iterate(id cluster, file* file) =0;
};

/**
 * indexed clusters keep the index in the successing file, as described below
 *
 * [ null ][ data0 ] - [ idx0 ][ data1 ] - [ idx1 ][ ... ]
 *
 * a more concrete example with 3 clusters is as follows:
 *
 * [  cluster 0 ] - [  cluster 1 ] - [  cluster 2 ]
 * [ I- ][  D0  ]   [ I0 ][  D0  ]   [ I1 ][  D2  ]   [ I2 ]
 * I- = blank (index for non-existent cluster -1; header for cluster 0)
 * D0 = content of cluster 0 (indexed in I0)
 * I0 = index for cluster 0 (back index for cluster 1, forward index for cluster 0)
 * D1 = content of cluster 1 (indexed in I1)
 * I1 = index for cluster 1 (back index for cluster 2, forward index for cluster 1)
 * D2 = content of cluster 2 (indexed in I2)
 * I2 = index for cluster 2 (header for as yet non-existent cluster 3, footer for cluster 2)
 *
 * opening cluster 1 (readonly):
 * [  cluster 0 ] - [  cluster 1 ] - [  cluster 2 ]
 * [ I- ][  D0  ]   [ I0 ][  D0  ]   [ I1 ][  D2  ]   [ I2 ]
 *                  AAAAAABBBBBBBB
 *  A:readonly index -'      '- B:cluster data
 *
 * resume writing (readwrite):
 * [  cluster 0 ] - [  cluster 1 ] - [  cluster 2 ]
 * [ I- ][  D0  ]   [ I0 ][  D0  ]   [ I1 ][  D2  ]   [ I2 ]
 *                                   AAAAAABBBBBBBB   CCCCCC
 *                   A:readonly index -'      |          '- C:(in-progress writable) index for D2
 *                                            '- B:(partial writable) cluster data
 */
class indexed_cluster final : public cluster {
public:
    using cluster::m_cluster;
    using cluster::m_file;
    using cluster::m_readonly;
    indexed_cluster_delegate* m_delegate;
    indexed_cluster(indexed_cluster_delegate* delegate, bool readonly) : cluster(delegate, readonly) {
        m_delegate = delegate;
    }
    void open(id cluster, bool readonly, bool clear = false) override;
    virtual void close() override;
    virtual void flush() override;
};

struct bitfield : public serializable {
    uint8_t* m_data;
    #define S(b) m_data[(b>>3)] |=  (1 << (b & 7))   // set
    #define U(b) m_data[(b>>3)] &= ~(1 << (b & 7))   // unset
    #define G(b) (m_data[(b>>3)] & (1 << (b & 7)))   // get

    size_t m_cap;
    bitfield(uint32_t cap) {
        if (cap == 0) cap = 1;
        m_cap = (cap + 7) >> 3;
        m_data = (uint8_t*)malloc(m_cap);
        m_data[m_cap - 1] = 0; // avoid random bits in out of bounds area, as even if user sets/unsets all cap bits, there may be some extraneous bits in the final byte
    }
    ~bitfield() { delete m_data; }
    inline void clear() { memset(m_data, 0, m_cap); }
    inline bool operator[](size_t idx) const { return bool(G(idx)); }
    inline void set(size_t idx) { S(idx); }
    inline void unset(size_t idx) { U(idx); }
    virtual void serialize(serializer* stream) const override { stream->write(m_data, m_cap); }
    virtual void deserialize(serializer* stream) override     { stream->read(m_data, m_cap); }

    #undef S
    #undef U
    #undef G
};

} // namespace cq

#endif // included_cq_io_h_
