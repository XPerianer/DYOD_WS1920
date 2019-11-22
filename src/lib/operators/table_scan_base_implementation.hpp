#pragma once

#include "types.hpp"
#include "../storage/table.hpp"

namespace opossum {
class TableScanBaseImplementation : private Noncopyable {
 public:
  TableScanBaseImplementation() = default;

  virtual ~TableScanBaseImplementation() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  TableScanBaseImplementation(TableScanBaseImplementation&&) = default;
  TableScanBaseImplementation& operator=(TableScanBaseImplementation&&) = default;

  virtual std::shared_ptr<const Table> execute(const std::shared_ptr<const Table> table,
                                               const ColumnID column_id, const ScanType scan_type,
                                               const AllTypeVariant search_value) = 0;
};

}  // namespace opossum
