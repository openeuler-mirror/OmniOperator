/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * @Description: md5 function implementations
*/

#ifndef OMNI_RUNTIME_MD5_H
#define OMNI_RUNTIME_MD5_H

#include <iostream>
#include <cstring>

namespace omniruntime::codegen::function {
class Md5Function {
public:
    static constexpr int MD5_HASH_LENGTH_BINARY = 16;

    Md5Function(const char *data, uint64_t len)
    {
        MD5Update(data, len);
    }

    //! Write the 16-byte (binary) digest to the specified location
    void Finish(unsigned char *outDigest);

    // Write the 32-character digest (in hexadecimal format) to the specified location
    void FinishHex(char *outDigest);

private:
    void MD5Update(const char *data, uint64_t len);

    static void DigestToBase16(const unsigned char *digest, char *zBuf);

    unsigned int buf[4] = {0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476};
    unsigned int bits[2] = {0, 0};
    unsigned char in[64] = {0};
};
}
#endif // OMNI_RUNTIME_MD5_H
