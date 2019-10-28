#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  _segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Given value count does not match column count");

  auto value_it = values.cbegin();
  auto value_end = values.cend();
  auto segment_it = _segments.cbegin();
  while(value_it != value_end) {
    (*segment_it)->append(*value_it);

    ++value_it;
    ++segment_it;
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  return _segments.at(column_id);
}

uint16_t Chunk::column_count() const {
  return _segments.size();
}

uint32_t Chunk::size() const {
  if (_segments.size() == 0) {
    return 0;
  }

  return _segments[0]->size();
}

}  // namespace opossum
