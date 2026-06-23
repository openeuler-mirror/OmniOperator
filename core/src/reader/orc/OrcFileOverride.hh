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

#ifndef ORC_FILE_REWRITE_HH
#define ORC_FILE_REWRITE_HH

#include <string>
#include <vector>

#include "orc/OrcFile.hh"
#include "reader/common/UriInfo.h"
#include "RegionCoalescer.h"

/** /file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace omniruntime::reader {

    // InputStream that prefetches coalesced regions into an in-memory cache.
    class PrefetchableInputStream : public ::orc::InputStream {
    public:
        // Best-effort: regions that fail to load are skipped.
        virtual void prefetchRegions(const std::vector<IoRegion> &regions) = 0;
    };

    /**
     * Create a input stream to a local file or HDFS file if path begins with "hdfs://"
     * @param uri the UriInfo of HDFS
     */
  std::unique_ptr<::orc::InputStream> readFileOverride(const UriInfo &uri, uint64_t filePreloadThreshold = 0);

  /**
   * Create a output stream to a local file or HDFS file if path begins with "hdfs://"
   * @param uri the UriInfo of HDFS
   */
  ORC_UNIQUE_PTR<::orc::OutputStream> writeFileOverride(const UriInfo &uri);

  /**
   * Create a input stream to an HDFS file.
   * @param uri the UriInfo of HDFS
   */
  std::unique_ptr<::orc::InputStream> createHdfsFileInputStream(const UriInfo &uri, uint64_t filePreloadThreshold = 0);

  /**
   * Create a output stream to an HDFS file.
   * @param uri the UriInfo of HDFS
   */
  std::unique_ptr<::orc::OutputStream> createHdfsFileOutputStream(const UriInfo &uri);
}

#endif
