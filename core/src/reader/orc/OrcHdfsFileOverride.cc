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

#include "OrcFileOverride.hh"
#include <sys/types.h>
#include <cstring>
#include <memory>
#include <new>
#include <vector>

#include "reader/filesystem/hdfs_file.h"
#include "reader/filesystem/io_exception.h"

namespace omniruntime::reader {

    using namespace fs;

class HdfsFileInputStreamOverride : public PrefetchableInputStream {
    private:
        std::string filename_;
        std::unique_ptr<fs::ReadableFile> hdfs_file_;
        uint64_t total_length_;
        const uint64_t READ_SIZE_ = 1024 * 1024; //1 MB

        struct CacheSeg {
            uint64_t offset;
            uint64_t length;
            std::unique_ptr<char[]> data;
        };
        std::vector<CacheSeg> cache_;
        bool wholeFileCached_ = false;

        // Returns true only if the full range was read.
        bool ReadRange(char *buffer, uint64_t length, uint64_t offset) {
            uint64_t done = 0;
            while (done < length) {
                int64_t n = hdfs_file_->ReadAt(buffer + done, length - done, offset + done);
                if (n < 0) {
                    return false;
                }
                if (n == 0) {
                    break;
                }
                done += n;
            }
            return done == length;
        }

    public:
        HdfsFileInputStreamOverride(const UriInfo& uri, uint64_t filePreloadThreshold) {
            this->filename_ = uri.Path();
            std::shared_ptr<HadoopFileSystem> fileSystemPtr = getHdfsFileSystem(uri);
            this->hdfs_file_ = std::make_unique<HdfsReadableFile>(fileSystemPtr, this->filename_, 0);

            Status openFileSt = hdfs_file_->OpenFile();
            if (!openFileSt.IsOk()) {
                throw IOException(openFileSt.ToString());
            }

            this->total_length_= hdfs_file_->GetFileSize();

            // Small file: preload the whole file in one IO.
            if (filePreloadThreshold > 0 && total_length_ > 0 && total_length_ <= filePreloadThreshold) {
                std::unique_ptr<char[]> buffer(new (std::nothrow) char[total_length_]);
                if (buffer && ReadRange(buffer.get(), total_length_, 0)) {
                    cache_.push_back(CacheSeg{0, total_length_, std::move(buffer)});
                    wholeFileCached_ = true;
                }
            }
        }

        void prefetchRegions(const std::vector<IoRegion> &regions) override {
            if (wholeFileCached_) {
                return;
            }
            cache_.clear();
            for (const auto &r : regions) {
                if (r.length == 0 || r.length > total_length_ || r.offset > total_length_ - r.length) {
                    continue;
                }
                std::unique_ptr<char[]> buffer(new (std::nothrow) char[r.length]);
                if (!buffer) {
                    continue;
                }
                if (!ReadRange(buffer.get(), r.length, r.offset)) {
                    continue;
                }
                cache_.push_back(CacheSeg{r.offset, r.length, std::move(buffer)});
            }
        }

        /**
         * get the total length of the file in bytes
         */
        uint64_t getLength() const override {
            return total_length_;
        }


        /**
         * get the natural size of reads
         */
        uint64_t getNaturalReadSize() const override {
            return READ_SIZE_;
        }

        /**
         * read length bytes from the file starting at offset into the buffer starting at buf
         * @param buf buffer save data
         * @param length the number of bytes to read
         * @param offset read from
         */
        void read(void *buf,
                  uint64_t length,
                  uint64_t offset) override {

            if (!buf) {
                throw IOException(Status::IOError("Fail to read hdfs file, because read buffer is null").ToString());
            }

            // Serve from a cache segment fully covering the range (overflow-safe check).
            for (const auto &seg : cache_) {
                if (length <= seg.length && offset >= seg.offset && (offset - seg.offset) <= (seg.length - length)) {
                    memcpy(buf, seg.data.get() + (offset - seg.offset), length);
                    return;
                }
            }

            char *buf_ptr = reinterpret_cast<char *>(buf);
            uint64_t total_bytes_read = 0;
            int64_t last_bytes_read = 0;

            do {
                last_bytes_read = hdfs_file_->ReadAt(buf_ptr, length - total_bytes_read,offset + total_bytes_read);
                if (last_bytes_read < 0) {
                    throw IOException(Status::IOError("Error reading bytes the file").ToString());
                }
                if (last_bytes_read == 0) {
                   break;
                }
                total_bytes_read += last_bytes_read;
                buf_ptr += last_bytes_read;
            } while (total_bytes_read < length);
        }

        const std::string &getName() const override {
            return filename_;
        }
    };

    std::unique_ptr<::orc::InputStream> createHdfsFileInputStream(const UriInfo &uri, uint64_t filePreloadThreshold) {
        return std::unique_ptr<::orc::InputStream>(
            new HdfsFileInputStreamOverride(uri, filePreloadThreshold));
    }

    class HdfsFileOutputStreamOverride : public ::orc::OutputStream {
    private:
        std::string filename_;
        std::unique_ptr<fs::WriteableFile> hdfs_file_;
        uint64_t total_length_{0};
        const uint64_t WRITE_SIZE_ = 1024 * 1024;

    public:
        explicit HdfsFileOutputStreamOverride(const UriInfo &uri) {
            this->filename_ = uri.Path();
            std::shared_ptr<HadoopFileSystem> fileSystemPtr = getHdfsFileSystem(uri);
            this->hdfs_file_ = std::make_unique<HdfsWriteableFile>(fileSystemPtr, this->filename_, 0);
            Status openFileSt = hdfs_file_->OpenFile();
            if (!openFileSt.IsOk()) {
                throw IOException(openFileSt.ToString());
            }

            this->total_length_ = hdfs_file_->GetFileSize();
        }

        ~HdfsFileOutputStreamOverride() override {}

        [[nodiscard]] uint64_t getLength() const override { return total_length_; }


        [[nodiscard]] uint64_t getNaturalWriteSize() const override { return WRITE_SIZE_; }

        void write(const void *buf, size_t length) override { hdfs_file_->Write(buf, length); }

        [[nodiscard]] const std::string &getName() const override { return filename_; }

        void close() override { hdfs_file_->Close(); }
    };

    std::unique_ptr<::orc::OutputStream> createHdfsFileOutputStream(const UriInfo &uri) {
        return std::unique_ptr<::orc::OutputStream>(new HdfsFileOutputStreamOverride(uri));
    }
}
