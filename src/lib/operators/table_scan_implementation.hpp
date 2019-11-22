#pragma once

#include <memory>
#include <utility>

#include "../storage/reference_segment.hpp"
#include "../storage/table.hpp"
#include "../storage/value_segment.hpp"
#include "table_scan_base_implementation.hpp"
#include "types.hpp"

namespace opossum {
template <typename T>
class TableScanImplementation : public TableScanBaseImplementation {
 public:
  TableScanImplementation(const std::shared_ptr<const Table> table, const ColumnID column_id, const ScanType scan_type,
                          const AllTypeVariant search_value)
      : TableScanBaseImplementation(table, column_id, scan_type, search_value) {
    // TODO: Do we want to use initializer lists for these?
    _typed_search_value = type_cast<T>(_search_value);
    _current_pos_list = std::make_shared<PosList>();

    // TODO: Do we want to set column names / column types in the table?
    _result_table = std::make_shared<Table>();

    // TODO: Maybe we don't need this for DictionarySegments or ReferenceSegments?
    _scan_type_comparator = _get_scan_type_comparator();
  }

  std::shared_ptr<const Table> on_execute() override {
    ChunkID num_chunks = _table->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; chunk_id++) {
      _process_chunk(chunk_id);
    }

    return _result_table;
  }

 protected:
  // Exctract the relevant segment from the chunk and pass it to _process_segment
  void _process_chunk(ChunkID chunk_id) {
    const Chunk& chunk = _table->get_chunk(chunk_id);

    // TODO: Is it correct that we only want _one_ output column in the resulting table?
    // I think usually a table scan gives all columns, but filters based on the selected column...

    const auto segment = chunk.get_segment(_column_id);

    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dictionary_segment) {
      _process_segment(chunk_id, dictionary_segment);
    }

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
      _process_segment(chunk_id, dictionary_segment);
    }

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    if (value_segment) {
      _process_segment(chunk_id, dictionary_segment);
    }

    throw new std::logic_error("Missing handling for segment type");
  }

  // These methods go through the segment and add all relevant values to the
  // _current_pos_list
  void _process_segment(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    throw new std::logic_error("Missing handling for segment type");
  }

  void _process_segment(ChunkID chunk_id, std::shared_ptr<ReferenceSegment> segment) {
    throw new std::logic_error("Missing handling for segment type");
  }

  void _process_segment(ChunkID chunk_id, std::shared_ptr<ValueSegment<T>> segment) {
    const auto& values = segment->values();
    const size_t values_size = values.size();

    // TODO: Do we want to have the size check in this loop? Feels expensive to me.
    for (size_t value_index = 0; value_index < values_size; ++value_index) {
      if (_scan_type_comparator(values[value_index])) {
        // TODO: Is the pointer access here expensive? Do we want some reserving?
        _current_pos_list->emplace_back(chunk_id, static_cast<ChunkOffset>(value_index));
      }
    }

    if (_current_pos_list->size() > _target_pos_list_size) {
      _finish_current_pos_list();
    }
  }

  // Make a ReferenceSegment out of the _current_pos_list, add it to the
  // _result_table and start a new _current_pos_list
  void _finish_current_pos_list() {
    const auto reference_segment = std::make_shared<ReferenceSegment>(_table, _column_id, _current_pos_list);

    auto chunk = Chunk();
    chunk.add_segment(reference_segment);

    _result_table->emplace_chunk(std::move(chunk));

    _current_pos_list = std::make_shared<PosList>();
  }

  std::function<bool(T)> _get_scan_type_comparator() {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 == _typed_search_value; };
      case ScanType::OpNotEquals:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 != _typed_search_value; };

      case ScanType::OpLessThan:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 < _typed_search_value; };
      case ScanType::OpLessThanEquals:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 <= _typed_search_value; };

      case ScanType::OpGreaterThan:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 > _typed_search_value; };
      case ScanType::OpGreaterThanEquals:
        return [&_typed_search_value = _typed_search_value](const T& val1) { return val1 >= _typed_search_value; };
    }

    throw new std::logic_error("Unhandled scan type");
  }

  // Members
  T _typed_search_value;
  std::function<bool(T)> _scan_type_comparator;

  size_t _target_pos_list_size;

  std::shared_ptr<PosList> _current_pos_list;
  std::shared_ptr<Table> _result_table;
};
}  // namespace opossum
