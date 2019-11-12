#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    vs_int->append(5);
    vs_int->append(5);
    vs_int->append(6);
    vs_int->append(6);
    vs_int->append(6);
    vs_int->append(4);

    vs_str->append("Bill");
    vs_str->append("Steve");
    vs_str->append("Alexander");
    vs_str->append("Steve");
    vs_str->append("Hasso");
    vs_str->append("Bill");
  }

  std::shared_ptr<ValueSegment<int>> vs_int = std::make_shared<ValueSegment<int>>();
  std::shared_ptr<ValueSegment<std::string>> vs_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  auto col = make_shared_by_data_type<BaseSegment, DictionarySegment>("string", vs_str);
  auto dict_col = std::dynamic_pointer_cast<DictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  std::shared_ptr<ValueSegment<int>> vs = std::make_shared<ValueSegment<int>>();
  for (int i = 0; i <= 10; i += 2) vs->append(i);
  auto col = make_shared_by_data_type<BaseSegment, DictionarySegment>("int", vs);
  auto dict_col = std::dynamic_pointer_cast<DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, AttributeVectorIsWideEnough) {
  std::shared_ptr<ValueSegment<int>> vs = std::make_shared<ValueSegment<int>>();
  vs->append(0);
  DictionarySegment<int> ds8(vs);
  EXPECT_EQ(ds8.attribute_vector()->width(), 1);

  for (int i = vs->size(); i < std::numeric_limits<uint8_t>::max() + 1; i++) {
    vs->append(i);
  }
  DictionarySegment<int> ds16(vs);
  EXPECT_EQ(ds16.attribute_vector()->width(), 2);

  for (int i = vs->size(); i < std::numeric_limits<uint16_t>::max() + 1; i++) {
    vs->append(i);
  }
  DictionarySegment<int> ds32(vs);
  EXPECT_EQ(ds32.attribute_vector()->width(), 4);
}

TEST_F(StorageDictionarySegmentTest, AccessWithSubscriptOperator) {
  DictionarySegment<int> ds_int(vs_int);
  EXPECT_EQ(ds_int[2], static_cast<AllTypeVariant>(6));

  DictionarySegment<std::string> ds_str(vs_str);
  EXPECT_EQ(ds_str[1], static_cast<AllTypeVariant>("Steve"));
}

TEST_F(StorageDictionarySegmentTest, AccessWithGet) {
  DictionarySegment<int> ds_int(vs_int);
  EXPECT_EQ(ds_int.get(2), 6);

  DictionarySegment<std::string> ds_str(vs_str);
  EXPECT_EQ(ds_str.get(1), "Steve");
}

TEST_F(StorageDictionarySegmentTest, ThrowsExceptionOnAppend) {
  DictionarySegment<int> ds(vs_int);
  EXPECT_THROW(ds.append(0), std::exception);
}

TEST_F(StorageDictionarySegmentTest, UniqueValuesCount) {
  DictionarySegment<int> ds(vs_int);
  EXPECT_EQ(ds.unique_values_count(), 3);
}

TEST_F(StorageDictionarySegmentTest, Size) {
  DictionarySegment<int> ds(vs_int);
  EXPECT_EQ(ds.size(), 6);
}

TEST_F(StorageDictionarySegmentTest, AccessValueByValueID) {
  DictionarySegment<int> ds(vs_int);
  EXPECT_EQ(ds.value_by_value_id(ValueID{0}), 4);
  EXPECT_EQ(ds.value_by_value_id(ValueID{1}), 5);
  EXPECT_EQ(ds.value_by_value_id(ValueID{2}), 6);
}

TEST_F(StorageDictionarySegmentTest, AccessValueByValueIDThrowsException) {
  DictionarySegment<int> ds(vs_int);
  EXPECT_THROW(ds.value_by_value_id(ValueID{3}), std::exception);
}

TEST_F(StorageDictionarySegmentTest, GetDictionary) {
  DictionarySegment<int> ds(vs_int);
  auto dictionary = ds.dictionary();
  EXPECT_EQ(dictionary->size(), 3);
  EXPECT_EQ(dictionary->at(0), 4);
}

TEST_F(StorageDictionarySegmentTest, GetAttributeVector) {
  DictionarySegment<int> ds(vs_int);
  auto attribute_vector = ds.attribute_vector();
  EXPECT_EQ(attribute_vector->size(), 6);
  EXPECT_EQ(attribute_vector->get(0), 1);
}

TEST_F(StorageDictionarySegmentTest, EstimateMemoryUsage) {
  std::shared_ptr<ValueSegment<int>> vs = std::make_shared<ValueSegment<int>>();
  DictionarySegment<int> ds_empty(vs);
  EXPECT_EQ(ds_empty.estimate_memory_usage(), 0);

  vs->append(0);
  DictionarySegment<int> ds8(vs);
  EXPECT_EQ(ds8.estimate_memory_usage(), 5);

  for (int i = vs->size(); i < std::numeric_limits<uint8_t>::max() + 1; i++) {
    vs->append(i);
  }
  DictionarySegment<int> ds16(vs);
  EXPECT_EQ(ds16.estimate_memory_usage(), 1536);
}

}  // namespace opossum
