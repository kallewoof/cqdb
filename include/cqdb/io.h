#ifndef included_cq_io_h_
#define included_cq_io_h_

#include <string>
#include <map>
#include <set>
#include <vector>

#include <cstdlib>
#include <cstdio>

#include <cqdb/uint256.h>

namespace cq {

class fs_error : public std::runtime_error { public: explicit fs_error(const std::string& str) : std::runtime_error(str) {} };
class io_error : public std::runtime_error { public: explicit io_error(const std::string& str) : std::runtime_error(str) {} };

bool mkdir(const std::string& path);
bool rmdir(const std::string& path);
bool rmfile(const std::string& path);
bool listdir(const std::string& path, std::vector<std::string>& list);
bool rmdir_r(const std::string& path);
void randomize(void* dst, size_t bytes);

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

class serializer;

class compressor {
public:
    virtual void compress(serializer* stm, const std::vector<uint256>& references) =0;
    virtual void compress(serializer* stm, const uint256& reference) =0;
    virtual void decompress(serializer* stm, std::vector<uint256>& references) =0;
    virtual void decompress(serializer* stm, uint256& reference) =0;
};

class serializer {
public:
    compressor* m_compressor{nullptr};

    virtual ~serializer() {}
    virtual bool eof() =0;
    virtual bool empty() { return tell() == 0 && eof(); }
    virtual size_t write(const uint8_t* data, size_t len) =0;
    virtual size_t read(uint8_t* data, size_t len) =0;
    virtual void seek(long offset, int whence) =0;
    virtual long tell() =0;
    uint8_t get_uint8();
    virtual void flush() {}

    // bitcoin core compatibility
    inline size_t write(const char* data, size_t len) { return write((const uint8_t*)data, len); }
    inline size_t read(char* data, size_t len) { return read((uint8_t*)data, len); }
    virtual int GetVersion() const { return 0; }

    template<typename T> inline size_t w(const T& data) { return write((const uint8_t*)&data, sizeof(T)); }
    template<typename T> inline size_t r(T& data)        { return read((uint8_t*)&data, sizeof(T)); }

    template<typename T> serializer& operator<<(const T& obj) { serialize(*this, obj); return *this; }
    //static_assert(sizeof(T) < sizeof(void*), "non-primitive objects will be serialized as is; call .serialize()"); if (sizeof(T) != w(obj)) throw fs_error("failed serialization"); return *this; }
    template<typename T> serializer& operator>>(T& obj) { deserialize(*this, obj); return *this; }
    // static_assert(sizeof(T) < sizeof(void*), "non-primitive objects will be deserialized as is; call .deserialize()"); if (sizeof(T) != r(obj)) throw fs_error("failed deserialization"); return *this; }

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
    virtual void serialize(serializer* stream) const override; \
    virtual void deserialize(serializer* stream) override

struct varint : public serializable {
    id m_value;
    explicit varint(id value = 0) : m_value(value) {}
    explicit varint(serializer* s) { deserialize(s); }
    prepare_for_serialization();
    static inline id load(serializer* s) { varint v(s); return v.m_value; }
};

template<typename T, typename Stream> void serialize(Stream& stm, const std::vector<T>& vec) {
    varint(vec.size()).serialize(&stm);
    for (const T& v : vec) serialize(stm, v);
}

template<typename T, typename Stream> void deserialize(Stream& stm, std::vector<T>& vec) {
    vec.resize(varint::load(&stm));
    for (size_t i = 0; i < vec.size(); ++i) deserialize(stm, vec[i]);
}

template<typename Stream> void serialize(Stream& stm, const serializable* ob) { ob->serialize(stm); }
template<typename Stream> void deserialize(Stream& stm, serializable* ob)     { ob->deserialize(stm); }

template<typename T, typename Stream> void serialize(Stream& stm, const T& ob) { ob.serialize(&stm); }
template<typename T, typename Stream> void deserialize(Stream& stm, T& ob)     { ob.deserialize(&stm); }

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

/**
 * Incmaps are efficiently encoded maps linking two ordered sequences together. The two
 * sequences must be increasing s.t. each key and value can be expressed as the previous
 * key and value + positive integers (one for the key and one for the value).
 */
struct incmap : serializable {
    std::map<id, id> m;
    prepare_for_serialization();
    bool operator==(const incmap& other) const;
    inline id at(id v) const { return m.at(v); }
    inline size_t count(id v) const { return m.count(v); }
    inline size_t size() const { return m.size(); }
    inline void clear() noexcept { m.clear(); }
};

struct unordered_set : serializable {
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
    cluster(cluster_delegate* delegate);
    ~cluster() override;
    virtual void open(id cluster, bool readonly, bool clear = false);
    virtual void close() {}
    virtual void resume_writing(bool clear = false);
    bool eof() override;
    size_t write(const uint8_t* data, size_t len) override;
    size_t read(uint8_t* data, size_t len) override;
    void seek(long offset, int whence) override;
    long tell() override;
    void flush() override { m_file->flush(); }
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
    indexed_cluster_delegate* m_delegate;
    indexed_cluster(indexed_cluster_delegate* delegate) : cluster(delegate) {
        m_delegate = delegate;
    }
    void open(id cluster, bool readonly, bool clear = false) override;
    virtual void close() override;
};

} // namespace cq

#endif // included_cq_io_h_
