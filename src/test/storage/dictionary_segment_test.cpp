#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"

namespace opossum {

// TODO: Perhaps we should create an easily testable DictionarySegment here too
class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<ValueSegment<int>> vc_int = std::make_shared<ValueSegment<int>>();
  std::shared_ptr<ValueSegment<std::string>> vc_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = make_shared_by_data_type<BaseSegment, DictionarySegment>("string", vc_str);
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
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = make_shared_by_data_type<BaseSegment, DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, AttributeVectorIsWideEnough) {
  vc_int->append(0);
  DictionarySegment<int> ds8(vc_int);
  EXPECT_EQ(ds8.attribute_vector()->width(), 1);

  for (int i = vc_int->size(); i < std::numeric_limits<uint8_t>::max() + 1; i++) {
    vc_int->append(i);
  }
  DictionarySegment<int> ds16(vc_int);
  EXPECT_EQ(ds16.attribute_vector()->width(), 2);

  for (int i = vc_int->size(); i < std::numeric_limits<uint16_t>::max() + 1; i++) {
    vc_int->append(i);
  }
  DictionarySegment<int> ds32(vc_int);
  EXPECT_EQ(ds32.attribute_vector()->width(), 4);
}

TEST_F(StorageDictionarySegmentTest, AccessWithSubscriptOperator) {
  vc_int->append(3);
  vc_int->append(5);
  vc_int->append(3);
  DictionarySegment<int> ds_int(vc_int);
  EXPECT_EQ(ds_int[2], static_cast<AllTypeVariant>(3));

  vc_str->append("Hasso");
  vc_str->append("Plattner");
  vc_str->append("Institute");
  DictionarySegment<std::string> ds_str(vc_str);
  EXPECT_EQ(ds_str[1], static_cast<AllTypeVariant>("Plattner"));
}

TEST_F(StorageDictionarySegmentTest, AccessWithGet) {
  vc_int->append(3);
  vc_int->append(5);
  vc_int->append(3);
  DictionarySegment<int> ds_int(vc_int);
  EXPECT_EQ(ds_int.get(2), 3);

  vc_str->append("Hasso");
  vc_str->append("Plattner");
  vc_str->append("Institute");
  DictionarySegment<std::string> ds_str(vc_str);
  EXPECT_EQ(ds_str.get(1), "Plattner");
}

TEST_F(StorageDictionarySegmentTest, ThrowsExceptionOnAppend) {
  DictionarySegment<int> ds(vc_int);
  EXPECT_THROW(ds.append(0), std::exception);
}

TEST_F(StorageDictionarySegmentTest, UniqueValuesCount) {
  vc_int->append(1);
  vc_int->append(2);
  vc_int->append(2);
  vc_int->append(3);
  vc_int->append(3);
  vc_int->append(3);

  DictionarySegment<int> ds(vc_int);
  EXPECT_EQ(ds.unique_values_count(), 3);
}

TEST_F(StorageDictionarySegmentTest, Size) {
  vc_int->append(1);
  vc_int->append(2);
  vc_int->append(2);
  vc_int->append(3);
  vc_int->append(3);
  vc_int->append(3);

  DictionarySegment<int> ds(vc_int);
  EXPECT_EQ(ds.size(), 6);
}

}  // namespace opossum
