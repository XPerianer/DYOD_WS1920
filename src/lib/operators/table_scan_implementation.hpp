#pragma once

#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

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
      : TableScanBaseImplementation(table, column_id, scan_type, search_value),
        _typed_search_value(type_cast<T>(_search_value)),
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
      const Chunk& chunk = _table->get_chunk(chunk_id);
      _process_chunk(chunk);
      _finish_current_chunk_offsets(chunk_id, chunk);
    }

    return _result_table;
  }

 protected:
  // Exctract the relevant segment from the chunk and pass it to _process_segment
  void _process_chunk(const Chunk& source_chunk) {
    const auto source_segment = source_chunk.get_segment(_column_id);

    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(source_segment);
    if (dictionary_segment) {
      _process_segment(dictionary_segment);
      return;
    }

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(source_segment);
    if (reference_segment) {
      _process_segment(reference_segment);
      return;
    }

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(source_segment);
    if (value_segment) {
      _process_segment(value_segment);
      return;
    }

    throw std::logic_error("Unhandled segment type in _process_chunk");
  }

  // -----------------
  // These methods go through the segment and add all relevant values to _chunk_offsets_to_add_to_result_table
  // OR set _add_all_chunk_offsets to prevent copying the whole list
  void _process_segment(std::shared_ptr<ValueSegment<T>> segment) {
    const auto& values = segment->values();
    const size_t values_size = values.size();

    for (size_t value_index = 0; value_index < values_size; ++value_index) {
      if (_scan_type_comparator(values[value_index])) {
        _chunk_offsets_to_add_to_result_table.push_back(value_index);
      }
    }
  }

  void _process_segment(std::shared_ptr<DictionarySegment<T>> segment) {
    const DictionarySegmentProcessingFlags flags(segment, _scan_type, _typed_search_value);

    if (flags.add_none) {
      return;
    }

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();

    if (flags.add_all) {
      _add_all_chunk_offsets = true;
    } else {
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        // This function call has some performance penalty (and we think the compiler won't optimize it)
        // But we think it's better than writing the same code 6 times. This should be benchmarked, though
        if (flags.should_add_value_id(attribute_vector.get(attribute_id))) {
          _chunk_offsets_to_add_to_result_table.push_back(attribute_id);
        }
      }
    }
  }

  void _process_segment(std::shared_ptr<ReferenceSegment> segment) {
    ColumnID referenced_column_id = segment->referenced_column_id();
    const std::shared_ptr<const Table> referenced_table = segment->referenced_table();
    const auto& pos_list = (*segment->pos_list());

    // Build a map: chunk_id to DictionarySegmentProcessingFlags (contains an entry if chunk at rowId.chunk_id
    // is DictionarySegment). This is necessary so when we loop over all referenced values in the PosList,
    // we can quickly decide whether an entry needs to be added to the result set.
    std::unordered_set<ChunkID> unique_referenced_chunk_ids;
    for (const auto& rowId : pos_list) {
      unique_referenced_chunk_ids.insert(rowId.chunk_id);
    }

    // initialize to true as we will and this together with some tests.
    // (Can we win the price for the longest variable names for this?)
    bool _all_referenced_segments_are_dictionary_segments_and_can_be_added_completely = true;

    std::unordered_map<ChunkID, DictionarySegmentProcessingFlags> referenced_dictionary_segment_flags;
    std::unordered_map<ChunkID, const std::vector<T>&> referenced_value_segment_vectors;
    std::unordered_map<ChunkID, std::shared_ptr<const BaseAttributeVector>> referenced_dictionary_segment_vectors;

    for (const auto& referenced_chunk_id : unique_referenced_chunk_ids) {
      const Chunk& referenced_chunk = referenced_table->get_chunk(referenced_chunk_id);
      const auto referenced_segment = referenced_chunk.get_segment(referenced_column_id);
      const auto referenced_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment);

      if (referenced_dictionary_segment != nullptr) {
        const DictionarySegmentProcessingFlags flags(referenced_dictionary_segment, _scan_type, _typed_search_value);
        referenced_dictionary_segment_flags.insert(std::make_pair(referenced_chunk_id, flags));
        referenced_dictionary_segment_vectors[referenced_chunk_id] = referenced_dictionary_segment->attribute_vector();

        _all_referenced_segments_are_dictionary_segments_and_can_be_added_completely &= flags.add_all;
      } else {
        _all_referenced_segments_are_dictionary_segments_and_can_be_added_completely = false;

        const auto referenced_value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment);
        DebugAssert(referenced_value_segment != nullptr,
                    "Unhandled segment type in _process_segment for ReferenceSegments");
        referenced_value_segment_vectors.insert(
            std::make_pair(referenced_chunk_id, referenced_value_segment->values()));
      }
    }

    // If all referenced segments are dictionary segments and each of them only has values
    // we want to add, we can exit early
    if (_all_referenced_segments_are_dictionary_segments_and_can_be_added_completely) {
      _add_all_chunk_offsets = true;
      return;
    }

    // Iterate over all RowIDs in the pos_list of the segment, for each entry, add it to
    // _chunk_offsets_to_add_to_result_table if the row at this chunk offset should be
    // in the result table.
    ChunkOffset chunk_size = segment->size();
    for (ChunkOffset chunk_offset = 0; chunk_offset < chunk_size; ++chunk_offset) {
      const RowID& row_id = pos_list[chunk_offset];
      const auto flags = referenced_dictionary_segment_flags.find(row_id.chunk_id);

      if (flags != referenced_dictionary_segment_flags.end()) {
        if (flags->second.add_none) {
          continue;
        }

        if (flags->second.add_all) {
          _chunk_offsets_to_add_to_result_table.push_back(chunk_offset);
          continue;
        }

        const auto attribute_vector_it = referenced_dictionary_segment_vectors.find(row_id.chunk_id);
        DebugAssert(attribute_vector_it != referenced_dictionary_segment_vectors.end(),
                    "Generation of referenced_dictionary_segment_vectors is broken.");
        if (flags->second.should_add_value_id(attribute_vector_it->second->get(chunk_offset))) {
          _chunk_offsets_to_add_to_result_table.push_back(chunk_offset);
        }
        continue;
      }

      const auto value_vector_it = referenced_value_segment_vectors.find(row_id.chunk_id);
      DebugAssert(value_vector_it != referenced_value_segment_vectors.end(),
                  "Unhandled segment type in _process_segment for ReferenceSegment");
      if (_scan_type_comparator(value_vector_it->second[chunk_offset])) {
        _chunk_offsets_to_add_to_result_table.push_back(chunk_offset);
      }
    }
  }

  // Make a ReferenceSegment out of the current _chunk_offsets_to_add_to_result_table,
  // add it to the _result_table and clear the _chunk_offsets_to_add_to_result_table
  void _finish_current_chunk_offsets(ChunkID chunk_id, const Chunk& source_chunk) {
    if (_chunk_offsets_to_add_to_result_table.size() == 0 && !_add_all_chunk_offsets) {
      // Table has a empty chunk by default. Don't append a new (semi) empty chunk
      return;
    }

    ColumnID column_count = static_cast<ColumnID>(_table->column_count());
    auto result_chunk = Chunk();

    // Will be lazily initialized if required
    std::shared_ptr<PosList> pos_list_for_value_and_dictionary_segment;

    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      const auto source_segment = source_chunk.get_segment(column_id);
      std::shared_ptr<ReferenceSegment> result_segment;

      const auto reference_source_segment = std::dynamic_pointer_cast<ReferenceSegment>(source_segment);
      if (reference_source_segment != nullptr) {
        if (_add_all_chunk_offsets) {
          // If we need to add all values of the source pos_list, we can just reference the same list.
          result_segment = std::make_shared<ReferenceSegment>(reference_source_segment->referenced_table(),
                                                              reference_source_segment->referenced_column_id(),
                                                              reference_source_segment->pos_list());
        } else {
          // TODO (anyone): Cache these instances per (table, source_pos_list) pair
          // TODO (anyone): Fix duplication here and in the else path
          auto pos_list = std::make_shared<PosList>();
          pos_list->reserve(_chunk_offsets_to_add_to_result_table.size());

          const auto& source_pos_list = *(reference_source_segment->pos_list());
          for (const ChunkOffset chunk_offset : _chunk_offsets_to_add_to_result_table) {
            pos_list->emplace_back(source_pos_list[chunk_offset]);
          }

          result_segment = std::make_shared<ReferenceSegment>(
              reference_source_segment->referenced_table(), reference_source_segment->referenced_column_id(), pos_list);
        }
      } else {
        if (pos_list_for_value_and_dictionary_segment == nullptr) {
          pos_list_for_value_and_dictionary_segment = std::make_shared<PosList>();

          if (_add_all_chunk_offsets) {
            ChunkOffset source_chunk_size = source_chunk.size();
            pos_list_for_value_and_dictionary_segment->reserve(source_chunk_size);
            for (ChunkOffset chunk_offset = 0; chunk_offset < source_chunk_size; ++chunk_offset) {
              pos_list_for_value_and_dictionary_segment->emplace_back(chunk_id, chunk_offset);
            }
          } else {
            pos_list_for_value_and_dictionary_segment->reserve(_chunk_offsets_to_add_to_result_table.size());

            for (const ChunkOffset chunk_offset : _chunk_offsets_to_add_to_result_table) {
              pos_list_for_value_and_dictionary_segment->emplace_back(chunk_id, chunk_offset);
            }
          }
        }

        result_segment =
            std::make_shared<ReferenceSegment>(_table, column_id, pos_list_for_value_and_dictionary_segment);
      }

      result_chunk.add_segment(result_segment);
    }

    _chunk_offsets_to_add_to_result_table.clear();
    _result_table->emplace_chunk(std::move(result_chunk));
    _add_all_chunk_offsets = false;
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

  struct DictionarySegmentProcessingFlags {
    DictionarySegmentProcessingFlags(const std::shared_ptr<DictionarySegment<T>> segment, const ScanType& _scan_type,
                                     const T& _typed_search_value) {
      ValueID matching_value_id;
      switch (_scan_type) {
        case ScanType::OpEquals:
          matching_value_id = segment->lower_bound(_typed_search_value);
          add_none = matching_value_id == INVALID_VALUE_ID ||
                     segment->value_by_value_id(matching_value_id) != _typed_search_value;
          should_add_value_id = [matching_value_id](ValueID id) { return id == matching_value_id; };
          break;
        case ScanType::OpNotEquals:
          matching_value_id = segment->lower_bound(_typed_search_value);
          add_all = matching_value_id == INVALID_VALUE_ID ||
                    segment->value_by_value_id(matching_value_id) != _typed_search_value;
          should_add_value_id = [matching_value_id](ValueID id) { return id != matching_value_id; };
          break;

        case ScanType::OpLessThan:
        case ScanType::OpLessThanEquals:
          // OpLessThanEquals is the same logic as OpLessThan, only that we need to use upper_bound instead of lower_bound
          matching_value_id = _scan_type == ScanType::OpLessThan ? segment->lower_bound(_typed_search_value)
                                                                 : segment->upper_bound(_typed_search_value);
          // We now want all elements in the attribute vector that have a value_id < matching_value_id.

          add_none = matching_value_id == ValueID(0);       // No lower values exist
          add_all = matching_value_id == INVALID_VALUE_ID;  // All values are lower
          should_add_value_id = [matching_value_id](ValueID id) { return id < matching_value_id; };
          break;

        case ScanType::OpGreaterThanEquals:
        case ScanType::OpGreaterThan:
          // OpGreaterThan is the same logic as OpGreaterThanEquals, only that we need to use upper_bound instead of lower_bound
          matching_value_id = _scan_type == ScanType::OpGreaterThanEquals ? segment->lower_bound(_typed_search_value)
                                                                          : segment->upper_bound(_typed_search_value);
          // We now want all elements in the attribute vector that have a value_id >= matching_value_id.

          add_none = matching_value_id == INVALID_VALUE_ID;  // All values are greater
          add_all = matching_value_id == ValueID(0);         // No values are greater
          should_add_value_id = [matching_value_id](ValueID id) { return id >= matching_value_id; };
          break;
      }
    }

    bool add_none = false;
    bool add_all = false;
    std::function<bool(ValueID)> should_add_value_id;
  };

  // Members
  T _typed_search_value;
  std::function<bool(T)> _scan_type_comparator;

  std::vector<ChunkOffset> _chunk_offsets_to_add_to_result_table;
  bool _add_all_chunk_offsets = false;

  std::shared_ptr<Table> _result_table;
};
}  // namespace opossum
