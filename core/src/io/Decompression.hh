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

#ifndef SPARK_DECOMPRESSION_HH
#define SPARK_DECOMPRESSION_HH

#include "lz4.h"
#include "zlib.h"
#include "zstd.h"
#include "wrap/snappy_wrapper.h"
#include <string>
#include <cstring>
#include <utility>

namespace omniSpark {
const int COMPRESS_BLOCK_SIZE = 64 * 1024;

enum DecompressCode {
    LZ4 = 0,
    ZLIB = 1,
    SNAPPY = 2,
    LZO = 3,
    ZSTD = 4
};

std::pair<char*, int32_t> decompressForCodec(const char* input, int32_t inputLenght,
   int32_t compressCode, int32_t shuffleCompressBlockSize);
std::pair<char*, int32_t> decompressLZ4(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize);
std::pair<char*, int32_t> decompressZlib(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize);
std::pair<char*, int32_t> decompressSnappy(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize);
std::pair<char*, int32_t> decompressZstd(const char* input, int32_t inputLength, int32_t shuffleCompressBlockSize);
}

#endif