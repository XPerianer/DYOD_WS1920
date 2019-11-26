#pragma once

#include <limits>
#include <memory>
#include <utility>

#include "../storage/reference_segment.hpp"
#include "../storage/table.hpp"
#include "../storage/value_segment.hpp"
#include "table_scan_base_implementation.hpp"
#include "types.hpp"

namespace {
inline void _debug_assert_all_segments_have_same_indirection(const opossum::Chunk& chunk,
                                                             bool should_be_reference_segments) {
  // Checks whether all segments are either (DictionarySegment or ValueSegment) or ReferenceSegment
  // Our code can currently not handle cases where these are mixed.
  uint16_t column_count = chunk.column_count();
  for (opossum::ColumnID column_id = opossum::ColumnID(0); column_id < column_count; ++column_id) {
    const auto base_segment = chunk.get_segment(column_id);
    const auto reference_segment_ptr = std::dynamic_pointer_cast<opossum::ReferenceSegment>(base_segment);
    bool is_reference_segment = reference_segment_ptr != nullptr;

    DebugAssert(is_reference_segment == should_be_reference_segments,
                "Table scan called with chunk with mixed segments");
  }
}
}  //unnamed namespace

namespace opossum {
template <typename T>
class TableScanImplementation : public TableScanBaseImplementation {
 public:
  TableScanImplementation(const std::shared_ptr<const Table> table, const ColumnID column_id, const ScanType scan_type,
                          const AllTypeVariant search_value)
      : TableScanBaseImplementation(table, column_id, scan_type, search_value),
        _typed_search_value(type_cast<T>(_search_value)),
        _current_pos_list(std::make_shared<PosList>()),
        _result_table(std::make_shared<Table>()) {
    ColumnID column_count = static_cast<ColumnID>(_table->column_count());
    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      _result_table->add_column(_table->column_name(column_id), _table->column_type(column_id));
    }

    _scan_type_comparator = _get_scan_type_comparator();
  }

  std::shared_ptr<const Table> on_execute() override {
    ChunkID num_chunks = _table->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; chunk_id++) {
      _process_chunk(chunk_id);
    }
    _finish_current_pos_list();

    return _result_table;
  }

 protected:
  // Exctract the relevant segment from the chunk and pass it to _process_segment
  void _process_chunk(ChunkID chunk_id) {
    const Chunk& chunk = _table->get_chunk(chunk_id);

    const auto segment = chunk.get_segment(_column_id);

    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dictionary_segment) {
      _debug_assert_all_segments_have_same_indirection(chunk, false);
      _set_referenced_table(_table);
      _process_segment(chunk_id, dictionary_segment);
      return;
    }

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
      _debug_assert_all_segments_have_same_indirection(chunk, true);
      _set_referenced_table(reference_segment->referenced_table());
      _process_segment(chunk_id, reference_segment);
      return;
    }

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    if (value_segment) {
      _debug_assert_all_segments_have_same_indirection(chunk, false);
      _set_referenced_table(_table);
      _process_segment(chunk_id, value_segment);
      return;
    }

    throw std::logic_error("Unhandled segment type in _process_chunk");
  }


  // -----------------
  // These methods go through the segment and add all relevant values to the _current_pos_list
  void _process_segment(ChunkID chunk_id, std::shared_ptr<ValueSegment<T>> segment) {
    const auto& values = segment->values();
    const size_t values_size = values.size();

    PosList& pos_list = *(_current_pos_list);

    for (size_t value_index = 0; value_index < values_size; ++value_index) {
      if (_scan_type_comparator(values[value_index])) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(value_index));
      }
    }

    if (_current_pos_list->size() > _target_pos_list_size) {
      _finish_current_pos_list();
    }
  }

  void _process_segment(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    ValueID matching_value_id;
    bool add_none = false;
    bool add_all = false;
    std::function<bool(ValueID)> should_add_value_id;
    _set_dictionary_segment_implementation_flags(segment, matching_value_id, add_none, add_all, should_add_value_id);

    if (add_none) {
      return;
    }

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (add_all) {
      pos_list.reserve(pos_list.size() + attribute_vector_size);
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else {
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        // This function call has some performance penalty (and we think the compiler won't optimize it)
        // But we think it's better than writing the same code 6 times. This should be benchmarked, though
        if (should_add_value_id(attribute_vector.get(attribute_id))) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }

    if (_current_pos_list->size() > _target_pos_list_size) {
      _finish_current_pos_list();
    }
  }

  void _process_segment(ChunkID chunk_id, std::shared_ptr<ReferenceSegment> segment) {
    ColumnID referenced_column_id = segment->referenced_column_id();
    const std::shared_ptr<const Table> referenced_table = segment->referenced_table();
    const auto& pos_list = (*segment->pos_list());

    // It's easier to handle all referenced values for a single chunk at once
    // The input may be sorted randomly(?), so in order to be able to process
    // the chunks this way, we need to regroup.
    std::unordered_map<ChunkID, std::vector<ChunkOffset>> regrouped_tuples;
    for (const auto& rowId : pos_list) {
      regrouped_tuples[rowId.chunk_id].push_back(rowId.chunk_offset);
    }

    for (const auto& [referenced_chunk_id, chunk_offsets] : regrouped_tuples) {
      const Chunk& chunk = referenced_table->get_chunk(referenced_chunk_id);
      const auto referenced_segment = chunk.get_segment(referenced_column_id);
      const auto referenced_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment);
      if (referenced_dictionary_segment != nullptr) {
        _update_current_pos_list_from_segment_offsets(referenced_chunk_id, referenced_dictionary_segment,
                                                      chunk_offsets);
        continue;
      }

      const auto referenced_value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment);
      if (referenced_value_segment != nullptr) {
        _update_current_pos_list_from_segment_offsets(referenced_chunk_id, referenced_value_segment, chunk_offsets);
        continue;
      }

      // All segments this reference segment points to should be DictionarySegment or ValueSegment.
      throw std::logic_error("Unhandled segment type in _process_segment for ReferenceSegment");
    }
  }

  void _update_current_pos_list_from_segment_offsets(ChunkID referenced_chunk_id,
                                                     std::shared_ptr<ValueSegment<T>> referenced_value_segment,
                                                     std::vector<ChunkOffset> chunk_offsets) {
    const auto& values = referenced_value_segment->values();
    auto& pos_list = *_current_pos_list;

    for (ChunkOffset chunk_offset : chunk_offsets) {
      const auto& value = values[chunk_offset];
      if (_scan_type_comparator(value)) {
        pos_list.emplace_back(referenced_chunk_id, chunk_offset);
      }
    }
  }

  void _update_current_pos_list_from_segment_offsets(
      ChunkID referenced_chunk_id, std::shared_ptr<DictionarySegment<T>> referenced_dictionary_segment,
      std::vector<ChunkOffset> chunk_offsets) {
    ValueID matching_value_id;
    bool add_none = false;
    bool add_all = false;
    std::function<bool(ValueID)> should_add_value_id;
    _set_dictionary_segment_implementation_flags(referenced_dictionary_segment, matching_value_id, add_none, add_all,
                                                 should_add_value_id);

    if (add_none) {
      return;
    }

    const auto& attribute_vector = *(referenced_dictionary_segment->attribute_vector());
    auto& pos_list = (*_current_pos_list);

    if (add_all) {
      pos_list.reserve(pos_list.size() + chunk_offsets.size());
      for (const auto chunk_offset : chunk_offsets) {
        pos_list.emplace_back(referenced_chunk_id, chunk_offset);
      }
    } else {
      for (const auto chunk_offset : chunk_offsets) {
        if (should_add_value_id(attribute_vector.get(chunk_offset))) {
          pos_list.emplace_back(referenced_chunk_id, chunk_offset);
        }
      }
    }
  }

  // Make a ReferenceSegment out of the _current_pos_list, add it to the _result_table and start a new _current_pos_list
  void _finish_current_pos_list() {
    _current_pos_list->shrink_to_fit();

    if (_current_pos_list->size() == 0) {
      // Table has a empty chunk by default. If _current_pos_list is empty, don't append a new (semi) empty chunk
      return;
    }

    auto chunk = Chunk();

    ColumnID column_count = static_cast<ColumnID>(_table->column_count());
    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      const auto reference_segment =
          std::make_shared<ReferenceSegment>(_current_referenced_table, column_id, _current_pos_list);
      chunk.add_segment(reference_segment);
    }

    _result_table->emplace_chunk(std::move(chunk));

    _current_pos_list = std::make_shared<PosList>();
  }

  // From now on, we are handling a new Segment, so maybe we're referencing another table.
  void _set_referenced_table(std::shared_ptr<const Table> table) {
    if (table != _current_referenced_table) {
      _finish_current_pos_list();
      _current_referenced_table = table;
    }
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

    throw std::logic_error("Unhandled scan type in _get_scan_type_comparator");
  }

  void _set_dictionary_segment_implementation_flags(std::shared_ptr<DictionarySegment<T>> segment,
                                                    ValueID& matching_value_id, bool& add_none, bool& add_all,
                                                    std::function<bool(ValueID)>& should_add_value_id) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        matching_value_id = segment->lower_bound(_typed_search_value);
        add_none = matching_value_id == INVALID_VALUE_ID ||
                   segment->value_by_value_id(matching_value_id) != _typed_search_value;
        should_add_value_id = [&matching_value_id](ValueID id) { return id == matching_value_id; };
        break;
      case ScanType::OpNotEquals:
        matching_value_id = segment->lower_bound(_typed_search_value);
        add_all = matching_value_id == INVALID_VALUE_ID ||
                  segment->value_by_value_id(matching_value_id) != _typed_search_value;
        should_add_value_id = [&matching_value_id](ValueID id) { return id != matching_value_id; };
        break;

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        // OpLessThanEquals is the same logic as OpLessThan, only that we need to use upper_bound instead of lower_bound
        matching_value_id = _scan_type == ScanType::OpLessThan ? segment->lower_bound(_typed_search_value)
                                                               : segment->upper_bound(_typed_search_value);
        // We now want all elements in the attribue vector that have a value_id < matching_value_id.

        add_none = matching_value_id == ValueID(0);       // No lower values exist
        add_all = matching_value_id == INVALID_VALUE_ID;  // All values are lower
        should_add_value_id = [&matching_value_id](ValueID id) { return id < matching_value_id; };
        break;

      case ScanType::OpGreaterThanEquals:
      case ScanType::OpGreaterThan:
        // OpGreaterThan is the same logic as OpGreaterThanEquals, only that we need to use upper_bound instead of lower_bound
        matching_value_id = _scan_type == ScanType::OpGreaterThanEquals ? segment->lower_bound(_typed_search_value)
                                                                        : segment->upper_bound(_typed_search_value);
        // We now want all elements in the attribue vector that have a value_id >= matching_value_id.

        add_none = matching_value_id == INVALID_VALUE_ID;  // All values are greater
        add_all = matching_value_id == ValueID(0);         // No values are greater
        should_add_value_id = [&matching_value_id](ValueID id) { return id >= matching_value_id; };
        break;
    }
  }

  // Members
  T _typed_search_value;
  std::function<bool(T)> _scan_type_comparator;

  // TODO: What is an appropriate value here?
  size_t _target_pos_list_size = std::numeric_limits<ChunkOffset>::max();

  std::shared_ptr<const Table> _current_referenced_table;
  std::shared_ptr<PosList> _current_pos_list;
  std::shared_ptr<Table> _result_table;
};
}  // namespace opossum
