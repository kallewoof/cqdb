#include <cqdb/cq.h>

#include <stdexcept>
#include <vector>

#include <sys/stat.h>
#include <errno.h>
#include <assert.h>

#ifdef _WIN32
#   include <direct.h>
#endif

namespace cq {

// header

header::header(uint8_t version, uint64_t timestamp, id cluster) : m_cluster(cluster), m_version(version), m_timestamp_start(timestamp) {}

void header::reset(uint8_t version, uint64_t timestamp, id cluster) {
    m_cluster = cluster;
    m_version = version;
    m_timestamp_start = timestamp;
    m_segments.clear();
}

header::header(id cluster, serializer* stream) : m_cluster(cluster) {
    deserialize(stream);
}

void header::serialize(serializer* stream) const {
    // MAGIC
    char magic[2];
    magic[0] = 'C'; magic[1] = 'Q';
    stream->write((uint8_t*)magic, 2);
    // VERSION
    stream->w(m_version);
    // TIMESTAMP
    stream->w(m_timestamp_start);
    // SEGMENTS
    *stream << m_segments;
}

void header::deserialize(serializer* stream) {
    // MAGIC
    char magic[2];
    stream->read(magic, 2);
    if (magic[0] != 'C' || magic[1] != 'Q') {
        char v[256];
        sprintf(v, "magic invalid (expected 'CQ', got '%c%c')", magic[0], magic[1]);
        throw db_error(v);
    }
    // VERSION
    stream->r(m_version);
    // TIMESTAMP
    stream->r(m_timestamp_start);
    // SEGMENTS
    *stream >> m_segments;
}

void header::mark_segment(id segment, id position) {
    m_segments.m[segment] = position;
}

id header::get_segment_position(id segment) const {
    return m_segments.at(segment);
}

size_t header::get_segment_count() const {
    return m_segments.size();
}

id header::get_first_segment() const {
    return m_segments.size() ? m_segments.m.begin()->first : 0;
}

id header::get_last_segment() const {
    return m_segments.size() ? m_segments.m.rbegin()->first : 0;
}

// registry

id registry::prepare_cluster_for_segment(id segment) {
    if (segment > m_tip) {
        if (m_clusters.m.size() == 0 || segment / m_cluster_size > m_tip / m_cluster_size) {
            m_clusters.m.insert(segment / m_cluster_size);
        }
        m_tip = segment;
    }
    return segment / m_cluster_size;
}

void registry::serialize(serializer* stream) const {
    // CLUSTER SIZE
    *stream << m_cluster_size;
    // CLUSTERS
    *stream << m_clusters;
    // TIP
    id sub = m_cluster_size * (m_clusters.m.size() ? *m_clusters.m.rbegin() : 0);
    assert(m_tip >= sub);
    *stream << varint(m_tip - sub);
}

void registry::deserialize(serializer* stream) {
    // CLUSTER SIZE
    *stream >> m_cluster_size;
    // CLUSTERS
    *stream >> m_clusters;
    // TIP
    id add = m_cluster_size * (m_clusters.m.size() ? *m_clusters.m.rbegin() : 0);
    m_tip = varint::load(stream) + add;
}

id registry::cluster_next(id cluster) {
    if (m_clusters.m.size() == 0) return nullid;
    id last = *m_clusters.m.rbegin();
    if (last <= cluster) return nullid;
    if (cluster + 32 < last) return cluster + 1;
    auto e = m_clusters.m.end();
    auto lower_bound = m_clusters.m.lower_bound(cluster);
    if (lower_bound == e) return nullid;
    lower_bound++;
    return lower_bound == e ? nullid : *lower_bound;
}

id registry::cluster_last(bool open_for_writing) {
    id last_cluster = m_clusters.m.size() == 0 ? nullid : *m_clusters.m.rbegin();
    if (open_for_writing && last_cluster == nullid) {
        last_cluster = 0;
        m_clusters.m.insert(0);
        m_current_cluster = 0;
    }
    return last_cluster;
}

std::string registry::cluster_path(id cluster) {
    char clu[30];
    sprintf(clu, "%05lld", cluster);
    return m_dbpath + "/" + m_prefix + clu + ".cq";
}

void registry::cluster_will_close(id cluster) {
    m_delegate->registry_closing_cluster(cluster);
}

void registry::cluster_opened(id cluster, file* file) {
    m_current_cluster = cluster;
    m_delegate->registry_opened_cluster(cluster, file);
}

void registry::cluster_write_forward_index(id cluster, file* file) {
    assert(cluster == m_current_cluster + 1);
    assert(cluster == m_forward_index.m_cluster);
    *file << m_forward_index;
}

void registry::cluster_read_forward_index(id cluster, file* file) {
    m_forward_index.m_cluster = cluster;
    *file >> m_forward_index;
}

void  registry::cluster_clear_forward_index(id cluster) {
    m_forward_index.reset(HEADER_VERSION, 0, cluster);
}

void registry::cluster_read_back_index(id cluster, file* file) {
    m_back_index.m_cluster = m_current_cluster = cluster;
    *file >> m_back_index;
}

void registry::cluster_clear_and_write_back_index(id cluster, file* file) {
    m_back_index.reset(HEADER_VERSION, 0, cluster);
    *file << m_back_index;
}

// db

db::db(const std::string& dbpath, const std::string& prefix, uint32_t cluster_size, bool readonly)
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

void db::open(id cluster, bool readonly) {
    if (!readonly && m_readonly) throw db_error("readonly database");
    m_ic.open(cluster, readonly);
}

void db::load() {
    m_ic.resume(false);
}

db::~db() {
    if (!m_readonly) {
        file regfile(m_dbpath + "/cq.registry", false, true);
        regfile << m_reg;
    }
    m_ic.close();
}

void db::registry_closing_cluster(id cluster) {}

void db::registry_opened_cluster(id cluster, file* file) {
    m_file = file;
    if (m_readonly) assert(m_file->readonly());
}

//
// registry
//

id db::store(object* t) {
    if (!m_file) throw db_error("invalid operation -- db not ready (no segment begun)");
    if (m_readonly) throw db_error("readonly database");
    if (m_file->readonly()) throw db_error("file is readonly");
    assert(t);
    id rval = m_file->tell();
    *m_file << *t;
    t->m_sid = rval;
    return rval;
}

void db::load(object* t) {
    assert(m_file);
    assert(t);
    id rval = m_file->tell();
    *m_file >> *t;
    t->m_sid = rval;
}

void db::fetch(object* t, id i) {
    assert(m_file);
    assert(t);
    long p = m_file->tell();
    if (p != i) m_file->seek(i, SEEK_SET);
    *m_file >> *t;
    if (p != m_file->tell()) m_file->seek(p, SEEK_SET);
    t->m_sid = i;
}

void db::refer(id sid) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    assert(sid < m_file->tell());
    *m_file << varint(m_file->tell() - sid);
}

void db::refer(object* t) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    assert(t);
    assert(t->m_sid != unknownid);
    assert(t->m_sid < m_file->tell());
    *m_file << varint(m_file->tell() - t->m_sid);
}

void db::refer(const uint256& hash) {
    if (m_readonly) throw db_error("readonly database");
    assert(m_file);
    hash.Serialize(*m_file);
}

id db::derefer() {
    assert(m_file);
    return m_file->tell() - varint::load(m_file);
}

uint256& db::derefer(uint256& hash) {
    assert(m_file);
    hash.Unserialize(*m_file);
    return hash;
}

void db::refer(object** ts, size_t sz) {
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
            ts[i]->m_hash.Serialize(*m_file);
        }
    }
}

void db::derefer(std::set<id>& known_out,  std::set<uint256>& unknown_out) {
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
        uint256 h;
        h.Unserialize(*m_file);
        unknown_out.insert(h);
    }
}

void db::begin_segment(id segment_id) {
    if (m_readonly) throw db_error("readonly database");
    if (segment_id < m_reg.m_tip) throw db_error("may not begin a segment < current tip");
    id new_cluster = m_reg.prepare_cluster_for_segment(segment_id);
    assert(m_reg.m_tip == segment_id || !m_file);
    bool write_reg = false;
    if (new_cluster != m_reg.m_current_cluster || !m_file) {
        write_reg = true;
        m_ic.open(new_cluster, false);
    }
    m_reg.m_forward_index.mark_segment(segment_id, m_file->tell());
    if (write_reg) {
        file regfile(m_dbpath + "/cq.registry", false, true);
        regfile << m_reg;
    }
}

void db::goto_segment(id segment_id) {
    id new_cluster = m_reg.prepare_cluster_for_segment(segment_id);
    if (new_cluster != m_reg.m_current_cluster || !m_file) {
        m_ic.open(new_cluster, true);
    }
    id pos = m_reg.m_forward_index.get_segment_position(segment_id);
    m_file->seek(pos, SEEK_SET);
}

void db::flush() {
    if (m_readonly) throw db_error("readonly database");
    assert(m_ic.m_file == m_file);
    m_ic.flush();
    // if (m_file) m_file->flush();
}

} // namespace cq
