#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

void Table::add_column(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Implementation goes here
}

uint16_t Table::column_count() const {
  // At creation of the table, a chunk is created. It is not possible to delete a chunk (only to emplace it).
  // Thus, _chunks[0] should always be valid.
  return _chunks[0].column_count();
}

uint64_t Table::row_count() const {
  // TODO: static_cast necessary here?
  return std::accumulate(_chunks.cbegin(), _chunks.cend(), static_cast<uint64_t>(0), [](uint64_t sum, const Chunk& chunk){
      return sum + chunk.size();
  });
}

ChunkID Table::chunk_count() const {
  // ChunkID is uint32_t, the vector could theoretically be as big as a uint64_t can become.
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_it = std::find(column_names.cbegin(), column_names.cend(), column_name);
  if (column_it == column_names.cend ()) {
    throw std::runtime_error("Invalid column name");
  }

  return std::distance(column_names.cbegin(), column_it);
}

uint32_t Table::max_chunk_size() const {
  return _max_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  throw std::runtime_error("Implement Table::column_names()");
}

const std::string& Table::column_name(ColumnID column_id) const {
  throw std::runtime_error("Implement Table::column_name");
}

const std::string& Table::column_type(ColumnID column_id) const {
  throw std::runtime_error("Implement Table::column_type");
}

Chunk& Table::get_chunk(ChunkID chunk_id) { throw std::runtime_error("Implement Table::get_chunk"); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { throw std::runtime_error("Implement Table::get_chunk"); }

}  // namespace opossum
