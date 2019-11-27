#pragma once

#include <memory>

#include "../storage/table.hpp"
#include "types.hpp"

namespace opossum {
class TableScanBaseImplementation : private Noncopyable {
 public:
  TableScanBaseImplementation(const std::shared_ptr<const Table> table, const ColumnID column_id,
                              const ScanType scan_type, const AllTypeVariant search_value)
      : _table(table), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

  virtual ~TableScanBaseImplementation() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  TableScanBaseImplementation(TableScanBaseImplementation&&) = default;
  TableScanBaseImplementation& operator=(TableScanBaseImplementation&&) = default;

  virtual std::shared_ptr<const Table> on_execute() = 0;

 protected:
  std::shared_ptr<const Table> _table;
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;
};

}  // namespace opossum
