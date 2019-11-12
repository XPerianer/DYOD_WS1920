#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  auto return_value = _tables.insert({name, table});

  if (!return_value.second) {
    throw std::runtime_error("add_table called with already existing table name");
  }
}

void StorageManager::drop_table(const std::string& name) {
  size_t elements_erased = _tables.erase(name);  // Can be 0 or 1.

  if (!elements_erased) {
    throw std::runtime_error("delete_table called with non-existant table");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> result;
  result.reserve(_tables.size());
  for (const auto& table_pair : _tables) {
    result.push_back(table_pair.first);
  }

  return result;
}

void StorageManager::print(std::ostream& out) const {
  auto names = table_names();
  size_t count = names.size();

  out << "Database contains " << count << (count == 1 ? " table" : " tables") << "." << std::endl;
  for (const auto& name : names) {
    auto table = get_table(name);
    size_t column_count = table->column_count();
    size_t row_count = table->row_count();

    out << name << " with " << column_count << (column_count == 1 ? " column" : " columns") << " and " << row_count
        << (row_count == 1 ? " row" : " rows") << "." << std::endl;
  }
}

void StorageManager::reset() { _tables.clear(); }

}  // namespace opossum
