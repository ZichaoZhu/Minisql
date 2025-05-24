#include "buffer/clock_replacer.h"

#include "gtest/gtest.h"

TEST(ClockReplacerTest, SampleTest) {
  CLOCKReplacer clock_replacer(7);

  // unpin
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1);
  EXPECT_EQ(6, clock_replacer.Size());
  clock_replacer.Unpin(7);
  EXPECT_EQ(7, clock_replacer.Size());

  // victim
  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // pin
  clock_replacer.Pin(3);
  EXPECT_EQ(4, clock_replacer.Size());
  clock_replacer.Pin(4);
  EXPECT_EQ(3, clock_replacer.Size());
  clock_replacer.Pin(5);
  EXPECT_EQ(2, clock_replacer.Size());

  // unpin
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(4);

  // victim
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(7, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
}