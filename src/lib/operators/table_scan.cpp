#include "table_scan.hpp"
#include <resolve_type.hpp>
#include "../storage/table.hpp"
#include "table_scan_base_implementation.hpp"
#include "table_scan_implementation.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in, nullptr), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto type = _input_table_left()->column_type(_column_id);
  const auto implementation = make_unique_by_data_type<TableScanBaseImplementation, TableScanImplementation>(type);
  
  const auto table = _input_table_left();
  return implementation->execute(table, _column_id, _scan_type, _search_value);
}

}  // namespace opossum
