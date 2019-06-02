#include <cqdb/io.h>

#include <stdexcept>
#include <vector>

#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <limits>

namespace cq {

uint8_t serializer::get_uint8() {
    uint8_t v;
    if (read(&v, 1)) return v;
    throw fs_error(eof() ? "end of file" : "error reading from disk");
}

void varint::serialize(serializer* stream) const {
    int nel = (sizeof(id)*8+6)/7;
    int marker = nel;
    unsigned char tmp[nel];
    uint64_t n = m_value;
    for (;;) {
        --nel;
        tmp[nel] = (n & 0x7F) | (marker == nel + 1 ? 0x00 : 0x80);
        if (n <= 0x7F) break;
        n = (n >> 7) - 1;
    }
    stream->write(&tmp[nel], marker - nel);
}

void varint::deserialize(serializer* stream) {
    m_value = 0;
    while(true) {
        uint8_t chData = stream->get_uint8();
        if (m_value > (std::numeric_limits<id>::max() >> 7)) {
           throw io_error("varint::deserialize(): size too large");
        }
        m_value = (m_value << 7) | (chData & 0x7F);
        if (chData & 0x80) {
            if (m_value == std::numeric_limits<id>::max()) {
                throw io_error("varint::deserialize(): size too large");
            }
            m_value++;
        } else return;
    }
}

void incmap::serialize(serializer* stream) const {
    // VARINT : number of entries
    *stream << varint((id)(m.size()));
    // serialize as varints equal to the diff with the previous element
    // TODO: use binomial encoding or something
    varint v;
    id lv = 0;
    for (const auto& kv : m) {
        assert(kv.first >= lv);
        v.m_value = kv.first - lv;
        lv = kv.first;
        *stream << v;
    }
    lv = 0;
    for (const auto& kv : m) {
        assert(kv.second >= lv);
        v.m_value = kv.second - lv;
        lv = kv.second;
        *stream << v;
    }
}

void incmap::deserialize(serializer* stream) {
    // VARINT : number of entries
    id size = varint::load(stream);
    // deserialize as varints equal to the diff with the previous element
    // TODO: use binomial encoding or something
    std::vector<id> k;
    k.resize(size);
    m.clear();
    id lv = 0; for (id i = 0; i < size; ++i) { lv += varint::load(stream); k[i] = lv; }
    lv = 0;    for (id i = 0; i < size; ++i) { lv += varint::load(stream); m[k[i]] = lv; }
}

bool incmap::operator==(const incmap& other) const {
    return m == other.m;
    // if (m.size() != other.m.size()) return false;
    // for (const auto& kv : m) {
    //     if (other.m.count(kv.first) == 0) return false;
    //     if (other.m.at(kv.first) != kv.second) return false;
    // }
    // return true;
}

void unordered_set::serialize(serializer* stream) const {
    // VARINT : number of entries
    *stream << varint((id)(m.size()));
    // serialize as varints equal to the diff with the previous element
    // TODO: use binomial encoding or something
    varint v;
    id lv = 0;
    for (const auto& k : m) {
        assert(k >= lv);
        v.m_value = k - lv;
        lv = k;
        *stream << v;
    }
}

void unordered_set::deserialize(serializer* stream) {
    // VARINT : number of entries
    id size = varint::load(stream);
    // deserialize as varints equal to the diff with the previous element
    // TODO: use binomial encoding or something
    m.clear();
    id lv = 0; for (id i = 0; i < size; ++i) { lv += varint::load(stream); m.insert(lv); }
}

// file stream

file::file(FILE* fp) : m_tell(0), m_fp(fp) {}

file::file(const std::string& fname, bool readonly, bool clear) {
    m_path = fname;
    m_readonly = readonly;
    m_tell = 0;
    m_fp = nullptr;
    if (!clear || readonly) {
        m_fp = fopen(m_path.c_str(), readonly ? "rb" : "rb+");
    }
    if (!m_fp && !readonly) m_fp = fopen(m_path.c_str(), "wb+");
    if (!m_fp) throw fs_error("cannot open file " + m_path);
}

bool file::accessible(const std::string& fname) {
    FILE* fp = fopen(fname.c_str(), "r");
    if (fp) {
        fclose(fp);
        return true;
    }
    return false;
}

file::~file() {
    if (m_fp) {
        fclose(m_fp);
        m_fp = nullptr;
    }
}

bool file::eof() {
    uint8_t byte;
    size_t s = fread(&byte, 1, 1, m_fp);
    if (s == 0) return true;
    fseek(m_fp, -1, SEEK_CUR);
    return false;
}

size_t file::write(const uint8_t* data, size_t len) {
    assert(!m_readonly);
    size_t w = fwrite(data, 1, len, m_fp);
    if (w != len) throw io_error("write error");
    m_tell += w;
    return w;
}

size_t file::read(uint8_t* data, size_t len) {
    size_t r = fread(data, 1, len, m_fp);
    if (r != len) throw io_error("end of file");
    m_tell += r;
    return r;
}

void file::seek(long offset, int whence) {
    fseek(m_fp, offset, whence);
    // force "fix" in case we went over the edge
    uint8_t b;
    size_t s = fread(&b, 1, 1, m_fp);
    if (s == 1) fseek(m_fp, -1, SEEK_CUR); else fseek(m_fp, 0, SEEK_END);
    m_tell = ftell(m_fp);
}

long file::tell() { return m_tell; }

void file::reopen() {
    m_tell = ftell(m_fp);
    fclose(m_fp);
    m_fp = fopen(m_path.c_str(), m_readonly ? "rb" : "rb+");
    fseek(m_fp, m_tell, SEEK_SET);
}

// char vector stream

bool chv_stream::eof() { return m_tell == m_chv.size(); }

size_t chv_stream::write(const uint8_t* data, size_t len) {
    m_tell = m_chv.size() + len;
    m_chv.insert(m_chv.end(), data, data + len);
    return len;
}

size_t chv_stream::read(uint8_t* data, size_t len)        {
    size_t r = m_chv.size() - m_tell;
    if (r > len) r = len;
    if (len > r) throw io_error("end of file"); // len = r;
    memcpy(data, &m_chv.data()[m_tell], len);
    m_tell += len;
    return r;
}
void chv_stream::seek(long offset, int whence) {
    if (whence == SEEK_SET) m_tell = offset;
    else if (whence == SEEK_CUR) m_tell += offset;
    else m_tell = m_chv.size() + offset;
    if (m_tell < 0) m_tell = 0;
    if (m_tell > m_chv.size()) m_tell = m_chv.size();
}
long chv_stream::tell() { return m_tell; }

// cluster stream

cluster::cluster(cluster_delegate* delegate, bool readonly) : m_delegate(delegate), m_file(nullptr), m_readonly(readonly) {}
cluster::~cluster()                                     { if (m_file) delete m_file; }
size_t cluster::write(const uint8_t* data, size_t len)  { return m_file->write(data, len); }
void cluster::seek(long offset, int whence)             { m_file->seek(offset, whence); }
long cluster::tell()                                    { return m_file->tell(); }

void cluster::open(id cluster, bool readonly, bool clear) {
    if (!readonly && m_readonly) throw io_error("readonly cluster");
    if (m_cluster != nullid) m_delegate->cluster_will_close(m_cluster);
    bool require_readonly = !clear && (m_cluster != nullid && cluster < m_cluster);
    if (require_readonly && !readonly) throw io_error("readonly mode required when opening target cluster (non-sequential operation requested)");
    m_cluster = cluster;
    if (m_file) delete m_file;
    m_file = new file(m_delegate->cluster_path(m_cluster), readonly, clear);
    m_delegate->cluster_opened(m_cluster, m_file);
}

bool cluster::eof() {
    if (m_cluster == nullid) return true;
    for (id next_cluster = m_delegate->cluster_next(m_cluster);
         m_file || next_cluster != nullid;
         next_cluster = m_delegate->cluster_next(m_cluster)) {
        if (m_file && !m_file->eof()) return false;
        if (next_cluster == nullid) return true;
        open(next_cluster, m_file->readonly());
    }
    return !m_file || m_file->eof();
}

size_t cluster::read(uint8_t* data, size_t len) {
    for (;;) {
        try {
            return m_file->read(data, len);
        } catch (io_error& err) {
            if (eof()) throw err;
        }
    }
}

void cluster::resume(bool clear) {
    open(m_delegate->cluster_last(!m_readonly), m_readonly, clear);
}

// indexed cluster

void indexed_cluster::close() {
    if (m_cluster != nullid) {
        m_delegate->cluster_will_close(m_cluster);
        if (!m_file->readonly()) {
            file forward_index(m_delegate->cluster_path(m_cluster + 1), false);
            m_delegate->cluster_write_forward_index(m_cluster + 1, &forward_index);
        }
    }
}

void indexed_cluster::flush() {
    cluster::flush();
    if (m_cluster != nullid && !m_file->readonly()) {
        file forward_index(m_delegate->cluster_path(m_cluster + 1), false);
        m_delegate->cluster_write_forward_index(m_cluster + 1, &forward_index);
    }
}

void indexed_cluster::open(id cluster, bool readonly, bool clear) {
    if (!readonly && m_readonly) throw io_error("readonly cluster");
    if (cluster == nullid) throw io_error("attempt to open nullid cluster");

    // 0. If readwrite open, write forward index (aka "close").
    close();

    if (m_file) delete m_file;

    if (readonly) {
        // 1. Read forward index. Open and read from cluster (x+1).
        if (file::accessible(m_delegate->cluster_path(cluster + 1))) {
            file forward_index(m_delegate->cluster_path(cluster + 1), true);
            m_delegate->cluster_read_forward_index(cluster + 1, &forward_index);
        } else {
            m_delegate->cluster_clear_forward_index(cluster + 1);
        }
        // 2. Open cluster x. Read back index.
        m_cluster = cluster;
        m_file = new file(m_delegate->cluster_path(m_cluster), true);
        m_delegate->cluster_read_back_index(m_cluster, m_file);
        m_delegate->cluster_opened(m_cluster,  m_file);
        return;
    }

    // for writing

	// 1. Read forward index from cluster x+1 if found.
    if (file::accessible(m_delegate->cluster_path(cluster + 1))) {
        file forward_index(m_delegate->cluster_path(cluster + 1), true);
        m_delegate->cluster_read_forward_index(cluster + 1, &forward_index);
    } else {
        m_delegate->cluster_clear_forward_index(cluster + 1);
    }

	// 2. Read back index from cluster x if found.
    m_cluster = cluster;
    m_file = new file(m_delegate->cluster_path(m_cluster), false);
    if (!m_file->eof()) {
        m_delegate->cluster_read_back_index(m_cluster, m_file);
        m_delegate->cluster_opened(m_cluster, m_file);
        // 3. Iterate cluster x to end.
        while (m_delegate->cluster_iterate(m_cluster, m_file));
    } else {
        m_delegate->cluster_clear_and_write_back_index(m_cluster, m_file);
        m_delegate->cluster_opened(m_cluster, m_file);
    }
}

// helper fun

long fsize(const std::string& path)
{
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) return -1;
    fseek(fp, 0, SEEK_END);
    long rv = ftell(fp);
    fclose(fp);
    return rv;
}

bool mkdir(const std::string& path)
{
    int rv =
#ifdef _WIN32
    ::_mkdir(path.c_str());
// #elif _POSIX_C_SOURCE
//     ::mkdir(path.c_str());
#else
    ::mkdir(path.c_str(), 0755);
#endif
    if (rv == 0) return true;
    switch (errno) {
    case EACCES: throw fs_error("permission denied");
    case EROFS: throw fs_error("read only filesystem");
    case EEXIST: return false;
    case 0: return true;
    default: throw fs_error("mkdir failed with error code " + std::to_string(errno));
    }
}

bool rmdir(const std::string& path)
{
    int rv =
#ifdef _WIN32
    ::_rmdir(path.c_str());
#else
    ::rmdir(path.c_str());
#endif
    return !rv;
    // switch (rv) {
    // case EACCES: throw fs_error("permission denied");
    // case EROFS: throw fs_error("read only filesystem");
    // case EEXIST: return false;
    // case 0: return true;
    // default: throw fs_error("mkdir failed");
    // }
}

bool rmfile(const std::string& path)
{
    return 0 == ::unlink(path.c_str());
}

bool listdir(const std::string& path, std::vector<std::string>& list)
{
#ifdef _WIN32
    WIN32_FIND_DATA ffd;
    auto hFind = FindFirstFile(path.c_str(), &ffd);
    if (hFind == INVALID_HANDLE_NAME) {
        throw fs_error("invalid handle name in call to FindFirstFile");
    }
    do {
        list.emplace_back(ffd.cFileName);
    } while (FindNextFile(hFind, &ffd) != 0);
    FindClose(hFind);
#else
    auto dirp = opendir(path.c_str());
    if (!dirp) return false;
    for (auto dp = readdir(dirp); dp; dp = readdir(dirp)) {
#ifdef _DIRENT_HAVE_D_NAMLEN
#   define len dp->d_namlen
#else
        size_t len = strlen(dp->d_name);
#endif
        if (dp->d_name[0] == '.' && len < 3 && (len == 1 || dp->d_name[1] == '.')) continue;
        list.emplace_back(dp->d_name, dp->d_name + len);
    }
    (void)closedir(dirp);
#endif
    return true;
}

bool rmdir_r(const std::string& path)
{
    std::vector<std::string> list;
    if (!listdir(path, list)) return false;
    for (const auto& file : list) {
        rmfile(path + "/" + file);
    }
    return rmdir(path);
}

void randomize(void* dst, size_t bytes)
{
    static FILE* urandom = nullptr;
    if (!urandom) {
        urandom = fopen("/dev/urandom", "rb");
        if (!urandom) throw std::runtime_error("cannot open /dev/urandom");
    }
    size_t rd = fread(dst, 1, bytes, urandom);
    if (rd != bytes) throw std::runtime_error("unable to read from /dev/urandom");
}

}
