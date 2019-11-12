#pragma once

#include <limits>
#include <vector>

#include "base_attribute_vector.hpp"

namespace opossum {

template <typename uintX_t>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(size_t size) : _values_ids(size) {}

  ValueID get(const size_t i) const override {
    DebugAssert(i < size(), "Index out of bounds");
    return ValueID{_values_ids[i]};
  }

  void set(const size_t i, const ValueID value_id) override {
    DebugAssert(i < size(), "Index out of bounds");
    _values_ids[i] = value_id;
  }

  size_t size() const override { return _values_ids.size(); }

  AttributeVectorWidth width() const override { return sizeof(uintX_t); }

 private:
  std::vector<uintX_t> _values_ids;
};

}  // namespace opossum
