#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _max_chunk_size(chunk_size) {
  // On table creation, a first chunk shall be created.
  _append_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "You can only add columns when no data has been added");

  _column_names.push_back(name);
  _column_types.push_back(type);

  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  _chunks.back().add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back().size() >= _max_chunk_size) {
    _append_new_chunk();
  }

  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  // Implementation goes here
}

uint16_t Table::column_count() const {
  // At creation of the table, a chunk is created. It is not possible to delete a chunk (only to emplace it).
  // Thus, _chunks[0] should always exist.
  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  return _chunks[0].column_count();
}

uint64_t Table::row_count() const {
  // static cast on the 0 is needed to make the compiler pick the correct type for the accumulate template.
  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  return std::accumulate(_chunks.cbegin(), _chunks.cend(), static_cast<uint64_t>(0),
                         [](uint64_t sum, const Chunk& chunk) { return sum + chunk.size(); });
}

ChunkID Table::chunk_count() const {
  // ChunkID is uint32_t, the vector could theoretically be as big as a uint64_t can become.
  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_it = std::find(_column_names.cbegin(), _column_names.cend(), column_name);
  if (column_it == _column_names.cend()) {
    throw std::runtime_error("Invalid column name");
  }

  // narrowing from long to ColumnID, we expect that we only have as many columns as ColumIDs can exist.
  return static_cast<ColumnID>(std::distance(_column_names.cbegin(), column_it));
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  return _chunks.at(chunk_id);
}

void Table::_append_new_chunk() {
  Chunk new_chunk;

  for (const auto& column_type : _column_types) {
    new_chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(column_type));
  }

  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  _chunks.push_back(std::move(new_chunk));
}

void Table::compress_chunk(ChunkID chunk_id) {
  const auto& uncompressed_chunk = get_chunk(chunk_id);
  Chunk compressed_chunk = Chunk();

  std::vector<std::future<std::shared_ptr<BaseSegment>>> compressed_segment_futures;

  const auto col_count = column_count();
  for (ColumnID column_id = ColumnID{0}; column_id < col_count; ++column_id) {
    const auto uncompressed_segment = uncompressed_chunk.get_segment(column_id);
    std::promise<std::shared_ptr<BaseSegment>> promise;
    compressed_segment_futures.push_back(promise.get_future());
    std::thread thread(_compress_segment, std::move(promise), column_type(column_id), uncompressed_segment);
    thread.detach();
  }

  for (auto& compressed_segment_future : compressed_segment_futures) {
    compressed_chunk.add_segment(compressed_segment_future.get());
  }

  const std::lock_guard<std::mutex> lock(_chunks_mutex);
  _chunks[chunk_id] = std::move(compressed_chunk);
}

void Table::_compress_segment(std::promise<std::shared_ptr<BaseSegment>> promise, const std::string type,
                              const std::shared_ptr<BaseSegment> uncompressed_segment) {
  promise.set_value(make_shared_by_data_type<BaseSegment, DictionarySegment>(type, uncompressed_segment));
}

void emplace_chunk(Chunk chunk) {
  // Implementation goes here
}

}  // namespace opossum
