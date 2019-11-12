#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "value_segment.hpp"

namespace opossum {

class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(base_segment);
    DebugAssert(value_segment, "Invalid base segment passed to dictionary segment constructor");

    // First pass: Fill the dictionary
    // For faster lookup, we store the elements in a set first
    std::set<T> distinct_values;
    for (const auto& value : value_segment->values()) {
      distinct_values.insert(value);
    }

    // Build an ordered vector based on the set.
    _dictionary = std::make_shared<std::vector<T>>(distinct_values.cbegin(), distinct_values.cend());

    const auto values = value_segment->values();
    const size_t value_size = values.size();
    const size_t dictionary_size = _dictionary->size();

    if (dictionary_size <= std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint8_t>>(value_size);
    } else if (dictionary_size <= std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint16_t>>(value_size);
    } else if (dictionary_size <= std::numeric_limits<uint32_t>::max()) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint32_t>>(value_size);
    }
    DebugAssert(_attribute_vector, "Too many unique values");

    // Second pass: Fill the attribute vector
    for (size_t value_index = 0; value_index < value_size; value_index++) {
      // The set that we used to construct the _dictionary vector is ordered by the "less" comparator,
      // so the constructed _dictionary is already sorted. Thus, we can find the index for each
      // value in the dictionary in O(log(n)).
      const auto& value = values[value_index];
      const auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
      size_t dic_index = std::distance(_dictionary->cbegin(), it);
      _attribute_vector->set(value_index, static_cast<ValueID>(dic_index));
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    return (*_dictionary)[_attribute_vector->get(chunk_offset)];
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const { return (*_dictionary)[_attribute_vector->get(chunk_offset)]; }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    throw std::runtime_error("append() called on immutable dictionary segment");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);

    if (it != _dictionary->cend()) {
      return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
    }
    return INVALID_VALUE_ID;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const {
    return lower_bound(get<T>(value));
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);

    if (it != _dictionary->cend()) {
      return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
    }
    return INVALID_VALUE_ID;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const {
    return upper_bound(get<T>(value));
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final {
    // using at(0) is legal here even if the vector is empty as the
    // sizeof operator does not evaluate the expression, it is just used at
    // compile-time to find out what type the expression would evaluate to.
    return sizeof(_dictionary->at(0)) * _dictionary->size() + _attribute_vector->width() * _attribute_vector->size();
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
