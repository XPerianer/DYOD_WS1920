#include "gtest/gtest.h"

#include "../../lib/storage/fixed_size_attribute_vector.hpp"

class FixedSystemAttributeVectorTest : public ::testing::Test {};

TEST_F(FixedSystemAttributeVectorTest, GetAndSet) {
  opossum::FixedSizeAttributeVector<uint8_t> vec(3);
  vec.set(0, opossum::ValueID{3});
  vec.set(1, opossum::ValueID{8});
  vec.set(2, opossum::ValueID{1});

  EXPECT_EQ(vec.get(0), 3);
  EXPECT_EQ(vec.get(1), 8);
  EXPECT_EQ(vec.get(2), 1);

  if (IS_DEBUG) {
    EXPECT_THROW(vec.set(5, opossum::ValueID{0}), std::exception);
    EXPECT_THROW(vec.get(6), std::exception);
  }
}

TEST_F(FixedSystemAttributeVectorTest, Size) {
  opossum::FixedSizeAttributeVector<uint8_t> vec(5);
  EXPECT_EQ(vec.size(), 5);
}

TEST_F(FixedSystemAttributeVectorTest, Width) {
  opossum::FixedSizeAttributeVector<uint8_t> vec8(1);
  opossum::FixedSizeAttributeVector<uint16_t> vec16(1);
  opossum::FixedSizeAttributeVector<uint32_t> vec32(1);

  EXPECT_EQ(vec8.width(), 1);
  EXPECT_EQ(vec16.width(), 2);
  EXPECT_EQ(vec32.width(), 4);
}
