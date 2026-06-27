/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>
#include "gtest/gtest.h"
#include "reader/orc/RegionCoalescer.h"

using omniruntime::reader::IoRegion;
using omniruntime::reader::coalesceRegions;

namespace {
constexpr uint64_t KB = 1024;
constexpr uint64_t MB = 1024 * 1024;

void ExpectRegions(const std::vector<IoRegion> &actual, const std::vector<IoRegion> &expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].offset, expected[i].offset) << "offset mismatch at index " << i;
        EXPECT_EQ(actual[i].length, expected[i].length) << "length mismatch at index " << i;
    }
}
} // namespace

// Empty input produces empty output.
TEST(RegionCoalescer, Empty)
{
    auto merged = coalesceRegions({}, 512 * KB, 128 * MB);
    EXPECT_TRUE(merged.empty());
}

// A single region is returned unchanged.
TEST(RegionCoalescer, Single)
{
    auto merged = coalesceRegions({{100, 50}}, 512 * KB, 128 * MB);
    ExpectRegions(merged, {{100, 50}});
}

// Zero-length regions are dropped.
TEST(RegionCoalescer, ZeroLengthDropped)
{
    auto merged = coalesceRegions({{0, 0}, {100, 10}, {200, 0}}, 512 * KB, 128 * MB);
    ExpectRegions(merged, {{100, 10}});
}

// Physically adjacent regions (gap 0) merge into one.
TEST(RegionCoalescer, AdjacentMerged)
{
    auto merged = coalesceRegions({{0, 100}, {100, 100}}, 0, 128 * MB);
    ExpectRegions(merged, {{0, 200}});
}

// A gap no larger than maxDistance is bridged (gap bytes included in the block).
TEST(RegionCoalescer, GapWithinDistanceMerged)
{
    auto merged = coalesceRegions({{0, 100}, {600, 100}}, 512, 128 * MB);
    ExpectRegions(merged, {{0, 700}});
}

// A gap larger than maxDistance is not merged.
TEST(RegionCoalescer, GapBeyondDistanceNotMerged)
{
    auto merged = coalesceRegions({{0, 100}, {700, 100}}, 512, 128 * MB);
    ExpectRegions(merged, {{0, 100}, {700, 100}});
}

// maxDistance == 0 only merges touching/overlapping regions, not gapped ones.
TEST(RegionCoalescer, ZeroDistanceKeepsGap)
{
    auto merged = coalesceRegions({{0, 100}, {101, 100}}, 0, 128 * MB);
    ExpectRegions(merged, {{0, 100}, {101, 100}});
}

// A merge that would exceed maxBytes is rejected and the region stays separate.
TEST(RegionCoalescer, MaxBytesCapPreventsMerge)
{
    auto merged = coalesceRegions({{0, 100}, {100, 100}}, 512, 150);
    ExpectRegions(merged, {{0, 100}, {100, 100}});
}

// A single region larger than maxBytes is still emitted as-is (never split).
TEST(RegionCoalescer, OversizedSingleRegionKept)
{
    auto merged = coalesceRegions({{0, 1000}}, 512, 100);
    ExpectRegions(merged, {{0, 1000}});
}

// Overlapping regions merge and the length spans to the farthest end.
TEST(RegionCoalescer, OverlappingMerged)
{
    auto merged = coalesceRegions({{0, 100}, {50, 100}}, 0, 128 * MB);
    ExpectRegions(merged, {{0, 150}});
}

// A region fully contained in the previous one does not extend the block.
TEST(RegionCoalescer, ContainedRegion)
{
    auto merged = coalesceRegions({{0, 200}, {50, 50}}, 0, 128 * MB);
    ExpectRegions(merged, {{0, 200}});
}

// Unsorted input is sorted by offset before merging.
TEST(RegionCoalescer, UnsortedInputSorted)
{
    auto merged = coalesceRegions({{200, 50}, {0, 50}, {100, 50}}, 512, 128 * MB);
    ExpectRegions(merged, {{0, 250}});
}

// Several close regions chain into one; a far one starts a new block.
TEST(RegionCoalescer, ChainThenBreak)
{
    auto merged = coalesceRegions({{0, 100}, {150, 100}, {300, 100}, {100000, 100}}, 512, 128 * MB);
    ExpectRegions(merged, {{0, 400}, {100000, 100}});
}

// Reaching exactly maxBytes is allowed; one byte more starts a new block.
TEST(RegionCoalescer, MaxBytesBoundary)
{
    auto mergedFits = coalesceRegions({{0, 100}, {100, 100}}, 512, 200);
    ExpectRegions(mergedFits, {{0, 200}});

    auto mergedSplit = coalesceRegions({{0, 100}, {100, 100}}, 512, 199);
    ExpectRegions(mergedSplit, {{0, 100}, {100, 100}});
}
