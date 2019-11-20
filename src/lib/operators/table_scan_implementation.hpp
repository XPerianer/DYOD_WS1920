
#include "../storage/table.hpp"

namespace opossum {
  template <typename T>
  class TableScanImplementation {
    std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id,
                                         const ScanType scan_type, const T search_value) {
      return std::make_shared<Table>();
    }
  };
}  // namespace <opossum>
