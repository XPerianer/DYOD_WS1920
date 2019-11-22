#include "../storage/table.hpp"
#include "table_scan_base_implementation.hpp"
#include "../storage/value_segment.hpp"
#include "../storage/reference_segment.hpp"

namespace opossum {
template <typename T>
class TableScanImplementation : public TableScanBaseImplementation {
    TableScanImplementation(const std::shared_ptr<const Table> table,
                            const ColumnID column_id, const ScanType scan_type,
                            const AllTypeVariant search_value)
    : TableScanBaseImplementation(table, column_id, scan_type, search_value) {

      // TODO: Do we want to use initializer lists for these?
      _typed_search_value = get<T>(_search_value);
      _current_pos_list = std::make_shared<PosList>();

      // TODO: Do we want to set column names / column types in the table?
      _result_table = std::make_shared<Table>();
    }


  virtual std::shared_ptr<const Table> on_execute() override {
    ChunkID num_chunks = _table->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; chunk_id++) {
      _process_chunk(_table->get_chunk(chunk_id));
    }

    return _result_table;
  }

 protected:

  // Exctract the relevant segment from the chunk and pass it to _process_segment
  void _process_chunk(const Chunk& chunk) {
    const auto segment = chunk.get_segment(_column_id);

    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment>(segment);
    if (dictionary_segment) {
      _process_segment(dictionary_segment);
    }

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
      _process_segment(dictionary_segment);
    }

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment>(segment);
    if (value_segment) {
      _process_segment(dictionary_segment);
    }

    throw new std::logic_error("Missing handling for segment type");
  }


  // These methods go through the segment and add all relevant values to the
  // current _pos_list
  void _process_segment(std::shared_ptr<DictionarySegment<T>> segment) {
    throw new std::logic_error("Missing handling for segment type");
  }

  void _process_segment(std::shared_ptr<ReferenceSegment> segment) {
    throw new std::logic_error("Missing handling for segment type");
  }

  void _process_segment(std::shared_ptr<ValueSegment<T>> segment) {
    throw new std::logic_error("Missing handling for segment type");
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


  // Members
  T _typed_search_value;

  std::shared_ptr<PosList> _current_pos_list;
  std::shared_ptr<Table> _result_table;
};
}  // namespace opossum
