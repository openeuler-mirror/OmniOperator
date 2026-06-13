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

#include "Decompression.hh"
#include "util/omni_exception.h"

namespace omniSpark {

    std::pair<char*, int32_t> decompressForCodec(const char* input, int32_t inputLength,
        int32_t compressCode, int32_t shuffleCompressBlockSize)
    {
        if (compressCode == DecompressCode::LZ4) {
            return decompressLZ4(input, inputLength, shuffleCompressBlockSize);
        }
        else if (compressCode == DecompressCode::ZLIB) {
            return decompressZlib(input, inputLength, shuffleCompressBlockSize);
        }
        else if (compressCode == DecompressCode::SNAPPY) {
            return decompressSnappy(input, inputLength, shuffleCompressBlockSize);
        }
        else if (compressCode == DecompressCode::ZSTD) {
            return decompressZstd(input, inputLength, shuffleCompressBlockSize);
        }
        else {
            OMNI_FAIL("Unknown compression code: " + std::to_string(compressCode));
        }
    }

    std::pair<char*, int32_t> decompressLZ4(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize)
    {
        char* output = new char[shuffleCompressBlockSize];
        int actualLength = LZ4_decompress_safe(input, output, inputLength, shuffleCompressBlockSize);
        if (actualLength < 0) {
            delete[] output;
            OMNI_FAIL("LZ4 decompression failed");
        }
        return std::make_pair(output, actualLength);
    }

    std::pair<char*, int32_t> decompressZlib(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize)
    {
        char* output = new char[shuffleCompressBlockSize];

        z_stream stream;
        memset(&stream, 0, sizeof(stream));
        stream.zalloc = Z_NULL;
        stream.zfree = Z_NULL;
        stream.opaque = Z_NULL;

        int err = inflateInit2(&stream, -15);
        if (err != Z_OK) {
            delete[] output;
            OMNI_FAIL("Failed to initialize zlib decompression stream: " + std::string(zError(err)));
        }

        stream.next_in = (Bytef*)input;
        stream.avail_in = inputLength;
        stream.next_out = (Bytef*)output;
        stream.avail_out = shuffleCompressBlockSize;

        err = inflate(&stream, Z_NO_FLUSH);
        if (err != Z_STREAM_END && err != Z_OK) {
            delete[] output;
            inflateEnd(&stream);
            OMNI_FAIL("Failed to decompress data: " + std::string(stream.msg));
        }

        // Clean up the decompression stream
        err = inflateEnd(&stream);
        if (err != Z_OK) {
            delete[] output;
            OMNI_FAIL("Failed to clean up zlib decompression stream: " + std::string(zError(err)));
        }
        return std::make_pair(output, stream.total_out);
    }

    std::pair<char*, int32_t> decompressSnappy(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize)
    {
        size_t unCompressedSize;
        if (!snappy::GetUncompressedLength(input, inputLength, &unCompressedSize)) {
            OMNI_FAIL("Failed to get uncompressed length.");
        }

        char* output = new char[unCompressedSize];
        if (!snappy::RawUncompress(input, inputLength, output)) {
            delete[] output;
            OMNI_FAIL("Failed to decompress data.");
        }
        return std::make_pair(output, unCompressedSize);
    }

    std::pair<char*, int32_t> decompressZstd(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize)
    {
        auto actualLength = ZSTD_getDecompressedSize(input, inputLength);
        if (actualLength == 0) {
            OMNI_FAIL("ZSTD decompression size failed");
        }

        char* output = new char[actualLength];
        auto retCode = ZSTD_decompress(output, actualLength, input, inputLength);
        if (ZSTD_isError(retCode)) {
            delete[] output;
            OMNI_FAIL("ZSTD decompression failed:" + std::string(ZSTD_getErrorName(retCode)));
        }
        return std::make_pair(output, actualLength);
    }

}
