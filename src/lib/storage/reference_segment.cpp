#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const auto rowID = (*_pos_list)[chunk_offset];
  const auto& chunk = _referenced_table->get_chunk(rowID.chunk_id);
  const auto segment = chunk.get_segment(_referenced_column_id);
  return (*segment)[rowID.chunk_offset];
}

size_t ReferenceSegment::size() const { return _pos_list->size(); }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos_list; }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceSegment::estimate_memory_usage() const { return sizeof(RowID) * _pos_list->size(); }
}  // namespace opossum
