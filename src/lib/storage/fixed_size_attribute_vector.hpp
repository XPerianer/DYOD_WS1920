#pragma once

#include <vector>
#include <limits>

#include <utils/assert.hpp>

#include "base_attribute_vector.hpp"

namespace opossum {

// TODO: test dis
template <typename uintX_t>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
    ValueID get(const size_t i) const override {
        return ValueID{_values_ids.at(i)};
    }

    void set(const size_t i, const ValueID value_id) override {
        DebugAssert(i < size(), "Index out of bounds");
        _values_ids[i] = value_id;
    }

    void push_back(const ValueID value_id) {
        DebugAssert(static_cast<size_t>(value_id) <= std::numeric_limits<uintX_t>::max(), "Value Id is too large for FixedSizeAttributeVector");
        _values_ids.push_back(value_id);
    }

    void reserve(size_t new_cap) {
        _values_ids.resize(new_cap);
    }

    size_t size() const override {
        return _values_ids.size();
    }

    AttributeVectorWidth width() const override {
        return sizeof(uintX_t);
    }

 private:
    std::vector<uintX_t> _values_ids;
};

}