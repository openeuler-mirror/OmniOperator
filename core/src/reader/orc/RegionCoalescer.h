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

#ifndef OMNI_REGION_COALESCER_H
#define OMNI_REGION_COALESCER_H

#include <algorithm>
#include <cstdint>
#include <vector>

namespace omniruntime::reader {

// A byte range [offset, offset + length) in the file.
struct IoRegion {
    uint64_t offset;
    uint64_t length;
};

// Merge neighbouring ranges whose gap <= maxDistance, keeping each block <= maxBytes.
inline std::vector<IoRegion> coalesceRegions(std::vector<IoRegion> regions, uint64_t maxDistance, uint64_t maxBytes)
{
    std::vector<IoRegion> merged;
    if (regions.empty()) {
        return merged;
    }
    std::sort(regions.begin(), regions.end(),
        [](const IoRegion &a, const IoRegion &b) { return a.offset < b.offset; });

    for (const auto &r : regions) {
        if (r.length == 0) {
            continue;
        }
        if (merged.empty()) {
            merged.push_back(r);
            continue;
        }
        IoRegion &last = merged.back();
        uint64_t lastEnd = last.offset + last.length;
        uint64_t newEnd = std::max(lastEnd, r.offset + r.length);
        if (r.offset <= lastEnd + maxDistance && (newEnd - last.offset) <= maxBytes) {
            last.length = newEnd - last.offset;
        } else {
            merged.push_back(r);
        }
    }
    return merged;
}

} // namespace omniruntime::reader

#endif // OMNI_REGION_COALESCER_H
