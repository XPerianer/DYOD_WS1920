#include "../storage/table.hpp"
#include "table_scan_base_implementation.hpp"

namespace opossum {
template <typename T>
class TableScanImplementation : public TableScanBaseImplementation {
  virtual std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in,
                                               const ColumnID column_id, const ScanType scan_type,
                                               const AllTypeVariant search_value) override {
    return std::make_shared<Table>();
  }
};
}  // namespace opossum
