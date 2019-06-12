#include <cqdb/cq.h>

extern "C" { void libcqdb_is_present(void) {} } // hello autotools, pleased to meat you

namespace cq {

// header

header::header(uint8_t version, id cluster) : m_cluster(cluster), m_version(version) {}

void header::reset(uint8_t version, id cluster) {
    m_cluster = cluster;
    m_version = version;
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
    // SEGMENTS
    *stream >> m_segments;
}

void header::mark_segment(id segment, id position) {
    m_segments.m[segment] = position;
}

id header::get_segment_position(id segment) const {
    return m_segments.at(segment);
}

bool header::has_segment(id segment) const {
    return m_segments.count(segment) > 0;
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
    if (segment > m_tip || (m_tip == 0 && m_clusters.m.size() == 0)) {
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
    m_forward_index.reset(HEADER_VERSION, cluster);
}

void registry::cluster_read_back_index(id cluster, file* file) {
    m_back_index.m_cluster = m_current_cluster = cluster;
    *file >> m_back_index;
}

void registry::cluster_clear_and_write_back_index(id cluster, file* file) {
    m_back_index.reset(HEADER_VERSION, cluster);
    *file << m_back_index;
}

}
