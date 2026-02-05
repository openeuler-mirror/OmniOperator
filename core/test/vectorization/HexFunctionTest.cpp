/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Hex function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <cstdint>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/HexFunctions.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

// Initialize function registration before running tests
class HexTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const hex_test_env = ::testing::AddGlobalTestEnvironment(new HexTestEnvironment);

// ==================== HexUtil Unit Tests ====================

TEST(HexUtilTest, ToHexBigint) {
    std::cout << "=== Test: HexUtil::toHex(int64_t) ===" << std::endl;
    
    std::string result;
    
    // Test positive numbers
    HexUtil::toHex(17, result);
    EXPECT_EQ(result, "11") << "hex(17) should be '11'";
    std::cout << "hex(17) = " << result << std::endl;
    
    HexUtil::toHex(255, result);
    EXPECT_EQ(result, "FF") << "hex(255) should be 'FF'";
    std::cout << "hex(255) = " << result << std::endl;
    
    HexUtil::toHex(256, result);
    EXPECT_EQ(result, "100") << "hex(256) should be '100'";
    std::cout << "hex(256) = " << result << std::endl;
    
    // Test zero
    HexUtil::toHex(0, result);
    EXPECT_EQ(result, "0") << "hex(0) should be '0'";
    std::cout << "hex(0) = " << result << std::endl;
    
    // Test negative numbers (two's complement)
    HexUtil::toHex(-1, result);
    EXPECT_EQ(result, "FFFFFFFFFFFFFFFF") << "hex(-1) should be 'FFFFFFFFFFFFFFFF'";
    std::cout << "hex(-1) = " << result << std::endl;
    
    HexUtil::toHex(-17, result);
    EXPECT_EQ(result, "FFFFFFFFFFFFFFEF") << "hex(-17) should be 'FFFFFFFFFFFFFFEF'";
    std::cout << "hex(-17) = " << result << std::endl;
    
    // Test INT64_MAX and INT64_MIN
    HexUtil::toHex(INT64_MAX, result);
    EXPECT_EQ(result, "7FFFFFFFFFFFFFFF") << "hex(INT64_MAX) should be '7FFFFFFFFFFFFFFF'";
    std::cout << "hex(INT64_MAX) = " << result << std::endl;
    
    HexUtil::toHex(INT64_MIN, result);
    EXPECT_EQ(result, "8000000000000000") << "hex(INT64_MIN) should be '8000000000000000'";
    std::cout << "hex(INT64_MIN) = " << result << std::endl;
}

TEST(HexUtilTest, ToHexString) {
    std::cout << "=== Test: HexUtil::toHex(string_view) ===" << std::endl;
    
    std::string result;
    
    // Test empty string
    HexUtil::toHex(std::string_view(""), result);
    EXPECT_EQ(result, "") << "hex('') should be ''";
    std::cout << "hex('') = '" << result << "'" << std::endl;
    
    // Test "Spark SQL" (from Velox test)
    HexUtil::toHex(std::string_view("Spark SQL"), result);
    EXPECT_EQ(result, "537061726B2053514C") << "hex('Spark SQL') should be '537061726B2053514C'";
    std::cout << "hex('Spark SQL') = " << result << std::endl;
    
    // Test simple ASCII
    HexUtil::toHex(std::string_view("A"), result);
    EXPECT_EQ(result, "41") << "hex('A') should be '41'";
    std::cout << "hex('A') = " << result << std::endl;
    
    HexUtil::toHex(std::string_view("AB"), result);
    EXPECT_EQ(result, "4142") << "hex('AB') should be '4142'";
    std::cout << "hex('AB') = " << result << std::endl;
    
    // Test with special characters
    HexUtil::toHex(std::string_view("#"), result);
    EXPECT_EQ(result, "23") << "hex('#') should be '23'";
    std::cout << "hex('#') = " << result << std::endl;
}

// ==================== HexBigintFunction Tests ====================

TEST(HexFunctionTest, HexBigintBasic) {
    std::cout << "=== Test: HexBigintFunction basic ===" << std::endl;
    
    HexBigintFunction<int64_t> func;
    std::string result;
    
    // Positive numbers
    EXPECT_TRUE(func.call(result, 17));
    EXPECT_EQ(result, "11");
    std::cout << "hex(17) = " << result << std::endl;
    
    EXPECT_TRUE(func.call(result, 0));
    EXPECT_EQ(result, "0");
    std::cout << "hex(0) = " << result << std::endl;
    
    // Negative numbers
    EXPECT_TRUE(func.call(result, -1));
    EXPECT_EQ(result, "FFFFFFFFFFFFFFFF");
    std::cout << "hex(-1) = " << result << std::endl;
    
    EXPECT_TRUE(func.call(result, -17));
    EXPECT_EQ(result, "FFFFFFFFFFFFFFEF");
    std::cout << "hex(-17) = " << result << std::endl;
}

TEST(HexFunctionTest, HexBigintEdgeCases) {
    std::cout << "=== Test: HexBigintFunction edge cases ===" << std::endl;
    
    HexBigintFunction<int64_t> func;
    std::string result;
    
    // INT64_MAX
    EXPECT_TRUE(func.call(result, INT64_MAX));
    EXPECT_EQ(result, "7FFFFFFFFFFFFFFF");
    std::cout << "hex(INT64_MAX) = " << result << std::endl;
    
    // INT64_MIN
    EXPECT_TRUE(func.call(result, INT64_MIN));
    EXPECT_EQ(result, "8000000000000000");
    std::cout << "hex(INT64_MIN) = " << result << std::endl;
    
    // NULL handling
    EXPECT_FALSE(func.callNullable(result, nullptr));
    std::cout << "hex(NULL) returns false (NULL)" << std::endl;
}

// ==================== HexVarcharFunction Tests ====================

TEST(HexFunctionTest, HexVarcharBasic) {
    std::cout << "=== Test: HexVarcharFunction basic ===" << std::endl;
    
    HexVarcharFunction<std::string_view> func;
    std::string result;
    
    // Empty string
    std::string_view empty = "";
    EXPECT_TRUE(func.call(result, empty));
    EXPECT_EQ(result, "");
    std::cout << "hex('') = '" << result << "'" << std::endl;
    
    // "Spark SQL" - from Velox test
    std::string_view sparkSql = "Spark SQL";
    EXPECT_TRUE(func.call(result, sparkSql));
    EXPECT_EQ(result, "537061726B2053514C");
    std::cout << "hex('Spark SQL') = " << result << std::endl;
    
    // Single character
    std::string_view singleChar = "A";
    EXPECT_TRUE(func.call(result, singleChar));
    EXPECT_EQ(result, "41");
    std::cout << "hex('A') = " << result << std::endl;
}

TEST(HexFunctionTest, HexVarcharSpecialChars) {
    std::cout << "=== Test: HexVarcharFunction special characters ===" << std::endl;
    
    HexVarcharFunction<std::string_view> func;
    std::string result;
    
    // Test with special characters from Velox test: "Spark\x65\x21SQL"
    std::string input1 = "Sparke!SQL";  // \x65 = 'e', \x21 = '!'
    EXPECT_TRUE(func.call(result, std::string_view(input1)));
    EXPECT_EQ(result, "537061726B652153514C");
    std::cout << "hex('Sparke!SQL') = " << result << std::endl;
    
    // Test binary data
    std::string binaryData{'\x00', '\x01', '\xFF'};
    EXPECT_EQ(binaryData.size(), 3);
    EXPECT_TRUE(func.call(result, std::string_view(binaryData)));
    EXPECT_EQ(result, "0001FF");
    std::cout << "hex('\\x00\\x01\\xFF') = " << result << std::endl;
}

TEST(HexFunctionTest, HexVarcharNullHandling) {
    std::cout << "=== Test: HexVarcharFunction NULL handling ===" << std::endl;
    
    HexVarcharFunction<std::string_view> func;
    std::string result;
    
    EXPECT_FALSE(func.callNullable(result, nullptr));
    std::cout << "hex(NULL varchar) returns false (NULL)" << std::endl;
}

// ==================== Unicode Tests ====================

TEST(HexFunctionTest, HexUnicode) {
    std::cout << "=== Test: hex with Unicode ===" << std::endl;
    
    HexVarcharFunction<std::string_view> func;
    std::string result;
    
    // From Velox test: hex("Spark\u6570\u636ESQL") = "537061726BE695B0E68DAE53514C"
    // Note: \u6570 = 数, \u636E = 据 (Chinese characters)
    // In UTF-8: 数 = E6 95 B0, 据 = E6 8D AE
    std::string unicodeStr = "Spark\xE6\x95\xB0\xE6\x8D\xAE""SQL";
    EXPECT_TRUE(func.call(result, std::string_view(unicodeStr)));
    EXPECT_EQ(result, "537061726BE695B0E68DAE53514C");
    std::cout << "hex('Spark数据SQL') = " << result << std::endl;
}

