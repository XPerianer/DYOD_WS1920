#include <limits>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/value_segment.hpp"

namespace opossum {
class StorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<int> int_value_segment;
  ValueSegment<std::string> string_value_segment;
  ValueSegment<double> double_value_segment;
};

TEST_F(StorageValueSegmentTest, GetSize) {
  EXPECT_EQ(int_value_segment.size(), 0u);
  EXPECT_EQ(string_value_segment.size(), 0u);
  EXPECT_EQ(double_value_segment.size(), 0u);
}

TEST_F(StorageValueSegmentTest, AddValueOfSameType) {
  int_value_segment.append(3);
  EXPECT_EQ(int_value_segment.size(), 1u);

  string_value_segment.append("Hello");
  EXPECT_EQ(string_value_segment.size(), 1u);

  double_value_segment.append(3.14);
  EXPECT_EQ(double_value_segment.size(), 1u);
}

TEST_F(StorageValueSegmentTest, AddValueOfDifferentType) {
  int_value_segment.append(3.14);
  EXPECT_EQ(int_value_segment.size(), 1u);
  EXPECT_THROW(int_value_segment.append("Hi"), std::exception);

  string_value_segment.append(3);
  string_value_segment.append(4.44);
  EXPECT_EQ(string_value_segment.size(), 2u);

  double_value_segment.append(4);
  EXPECT_EQ(double_value_segment.size(), 1u);
  EXPECT_THROW(double_value_segment.append("Hi"), std::exception);
}

TEST_F(StorageValueSegmentTest, AccessUsingIndexOperator) {
  int_value_segment.append(3);
  int_value_segment.append(5);
  int_value_segment.append(3);
  EXPECT_EQ(int_value_segment[1], static_cast<AllTypeVariant>(5));

  string_value_segment.append("Hasso");
  string_value_segment.append("Plattner");
  string_value_segment.append("Institute");
  EXPECT_EQ(string_value_segment[1], static_cast<AllTypeVariant>("Plattner"));

  double_value_segment.append(3.14);
  double_value_segment.append(3.15);
  double_value_segment.append(3.16);
  EXPECT_EQ(double_value_segment[2], static_cast<AllTypeVariant>(3.16));
}

TEST_F(StorageValueSegmentTest, AccessOutOfBounds) {
  int_value_segment.append(3.14);
  EXPECT_THROW(int_value_segment[2], std::exception);
}

TEST_F(StorageValueSegmentTest, GetValues) {
  int_value_segment.append(1);
  int_value_segment.append(2);
  int_value_segment.append(24);

  const std::vector<int> expected_values = {1, 2, 24};
  const auto values = int_value_segment.values();

  EXPECT_TRUE(expected_values == values);
}

TEST_F(StorageValueSegmentTest, MemoryUsage) {
  int_value_segment.append(1);
  EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{4});
  int_value_segment.append(2);
  EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{8});

  double_value_segment.append(3.14);
  EXPECT_EQ(double_value_segment.estimate_memory_usage(), size_t{8});
  double_value_segment.append(42.42);
  EXPECT_EQ(double_value_segment.estimate_memory_usage(), size_t{16});
}
}  // namespace opossum
