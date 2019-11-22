#include "../storage/table.hpp"
#include "table_scan_base_implementation.hpp"
#include "../storage/value_segment.hpp"

namespace opossum {
template <typename T>
class TableScanImplementation : public TableScanBaseImplementation {
  virtual std::shared_ptr<const Table> execute(const std::shared_ptr<const Table> table,
                                               const ColumnID column_id, const ScanType scan_type,
                                               const AllTypeVariant search_value) override {

    std::shared_ptr<PosList> pos_list;

    ChunkID num_chunks = table->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < num_chunks; chunk_id++) {
      _process_chunk(table->get_chunk(chunk_id), column_id, pos_list);
    }

    return std::make_shared<Table>();
  }

 protected:
  void _process_chunk(const Chunk& chunk, const ColumnID column_id, std::shared_ptr<PosList> pos_list) {
    const auto segment = chunk.get_segment(column_id);
    // TODO: WIP
  }
};
}  // namespace opossum
