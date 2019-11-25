#pragma once

#include <memory>
#include <utility>
#include <limits>

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
        _current_pos_list(std::make_shared<PosList>()),
        _result_table(std::make_shared<Table>()) {

    ColumnID column_count = static_cast<ColumnID>(_table->column_count());
    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      _result_table->add_column_definition(_table->column_name(column_id), _table->column_type(column_id));
    }

    // TODO: Maybe we don't need this for DictionarySegments or ReferenceSegments?
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
      _process_segment(chunk_id, dictionary_segment);
      return;
    }

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
     _process_segment(chunk_id, reference_segment);
     return;
    }

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    if (value_segment) {
      _process_segment(chunk_id, value_segment);
      return;
    }

    throw std::logic_error("Missing handling for segment type");
  }

  // These methods go through the segment and add all relevant values to the
  // _current_pos_list
  void _process_segment(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        _process_dictionary_segment_equals(chunk_id, segment); break;
      case ScanType::OpNotEquals:
        _process_dictionary_segment_not_equals(chunk_id, segment); break;

      case ScanType::OpLessThan:
        _process_dictionary_segment_less_than(chunk_id, segment); break;
      case ScanType::OpLessThanEquals:
        _process_dictionary_segment_less_than_equals(chunk_id, segment); break;

      case ScanType::OpGreaterThan:
        _process_dictionary_segment_greater_than(chunk_id, segment); break;
      case ScanType::OpGreaterThanEquals:
        _process_dictionary_segment_greater_than_equals(chunk_id, segment); break;
    }

    if (_current_pos_list->size() > _target_pos_list_size) {
      _finish_current_pos_list();
    }
  }

  void _process_segment(ChunkID chunk_id, std::shared_ptr<ReferenceSegment> segment) {
    throw std::logic_error("Missing handling for segment type");
  }

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

  // Make a ReferenceSegment out of the _current_pos_list, add it to the
  // _result_table and start a new _current_pos_list
  void _finish_current_pos_list() {
    _current_pos_list->shrink_to_fit();

    if (_current_pos_list->size() == 0) {
      // Table has a empty chunk by default. If _current_pos_list is empty, don't append a new (semi) empty chunk
      return;
    }

    auto chunk = Chunk();

    ColumnID column_count = static_cast<ColumnID>(_table->column_count());
    for (ColumnID column_id = ColumnID(0); column_id < column_count; ++column_id) {
      const auto reference_segment = std::make_shared<ReferenceSegment>(_table, column_id, _current_pos_list);
      chunk.add_segment(reference_segment);
    }

    _result_table->emplace_chunk(std::move(chunk));

    _current_pos_list = std::make_shared<PosList>();

    // We will need to push back to this very often. We think that _target_pos_list_size
    // is probably a good estimation for how many elements we will have to push back.
    // When we are finishing this list, we will shrink the vector back.
    // We pray that this gives good performance for a little bit of memory.
    // TODO: It may be better to do count - reserve - push_back in the _process_segment
    // methods -- we should benchmark this.
    _current_pos_list->reserve(2 * _target_pos_list_size);
  }

  // TODO: This is probably faster if we inline it
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

    throw std::logic_error("Unhandled scan type");
  }

  // -----------------
  // Specific implementations on third level de-templating (e.g. equals for dictionary segments for int)

  void _process_dictionary_segment_equals(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    ValueID matching_value_id = segment->lower_bound(_typed_search_value);

    if (matching_value_id == INVALID_VALUE_ID || segment->value_by_value_id(matching_value_id) != _typed_search_value) {
      return;
    }

    auto& pos_list = (*_current_pos_list);
    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
      if (attribute_vector.get(attribute_id) == matching_value_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    }
  }

  void _process_dictionary_segment_not_equals(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    ValueID matching_value_id = segment->lower_bound(_typed_search_value);

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (matching_value_id == INVALID_VALUE_ID || segment->value_by_value_id(matching_value_id) != _typed_search_value) {
      // add all rows to the position list
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else {
      // add only the rows _not_ matching the search value to the position list
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        if (attribute_vector.get(attribute_id) != matching_value_id) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }
  }

  void _process_dictionary_segment_greater_than_equals(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    ValueID matching_value_id = segment->lower_bound(_typed_search_value);

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (matching_value_id == INVALID_VALUE_ID) {
      // No element in the dictionary is >= search value -> return no elements
    } else if (matching_value_id == ValueID(0)) {
      // All elements in the dictionary are >= search_value -> return all elements
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else {
      // Some elements are less than search_value, some are greater
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        if (attribute_vector.get(attribute_id) >= matching_value_id) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }
  }

  void _process_dictionary_segment_greater_than(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    // TODO: Deduplicate with greater_than_equals
    ValueID matching_value_id = segment->upper_bound(_typed_search_value);

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (matching_value_id == INVALID_VALUE_ID) {
      // No element in the dictionary is > search value -> return no elements
    } else if (matching_value_id == ValueID(0)) {
      // All elements in the dictionary are > search_value -> return all elements
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else {
      // Some elements are less than search_value, some are greater
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        if (attribute_vector.get(attribute_id) >= matching_value_id) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }
  }

  void _process_dictionary_segment_less_than(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    ValueID matching_value_id = segment->lower_bound(_typed_search_value);

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (matching_value_id == INVALID_VALUE_ID) {
      // No element in the dictionary is >= search value -> return all elements
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else if (matching_value_id == ValueID(0)) {
      // All elements in the dictionary are >= search_value -> return no elements
    } else {
      // Some elements are less than search_value, some are greater
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        if (attribute_vector.get(attribute_id) < matching_value_id) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }
  }

  void _process_dictionary_segment_less_than_equals(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment) {
    // TODO: Deduplicate with less_than
    ValueID matching_value_id = segment->upper_bound(_typed_search_value);

    const auto& attribute_vector = *(segment->attribute_vector());
    size_t attribute_vector_size = attribute_vector.size();
    auto& pos_list = (*_current_pos_list);

    if (matching_value_id == INVALID_VALUE_ID) {
      // No element in the dictionary is >= search value -> return all elements
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
      }
    } else if (matching_value_id == ValueID(0)) {
      // All elements in the dictionary are >= search_value -> return no elements
    } else {
      // Some elements are less than search_value, some are greater
      for (size_t attribute_id = 0; attribute_id < attribute_vector_size; ++attribute_id) {
        if (attribute_vector.get(attribute_id) < matching_value_id) {
          pos_list.emplace_back(chunk_id, static_cast<ChunkOffset>(attribute_id));
        }
      }
    }
  }



  // Members
  T _typed_search_value;
  std::function<bool(T)> _scan_type_comparator;

  // TODO: What is an appropriate value here?
  size_t _target_pos_list_size = std::numeric_limits<ChunkOffset>::max();

  std::shared_ptr<PosList> _current_pos_list;
  std::shared_ptr<Table> _result_table;
};
}  // namespace opossum
