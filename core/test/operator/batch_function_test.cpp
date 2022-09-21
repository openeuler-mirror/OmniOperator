// /*
// * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
// * Description: ...
// */
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "../../src/codegen/functions/dictionaryfunctions.h"
#include "../../src/util/engine.h"
#include "../../src/codegen/batch_functions/batch_stringfunctions.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::codegen;
using namespace std;

TEST(BatchFunctionTest, SubstrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());

    vector<std::string> inputStr = { str, str, str, str, str, str, str, str };
    vector<int32_t> inputLen = { strLen, strLen, strLen, strLen, strLen, strLen, strLen, strLen };
    vector<int32_t> startIndexs = { 1, 1, 10, -5, 0, 37, -38, -37 };
    vector<int32_t> length = { 37, 5, 10, 7, 0, strLen + 5, 10, 37 };
    int32_t rowCnt = inputStr.size();
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> outResult(rowCnt);
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }
    bool isAnyNull[] = {false, false, false, false, false, false, false, false};
    BatchSubstr(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), length.data(), isAnyNull,
                outResult.data(), outLen.data(), rowCnt);

    vector<std::string> expected = { str, "时欧基乌斯", "hello! 回复哦", "色的圣诞袜", "", "袜", "", str };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    delete context;
}

TEST(BatchFunctionTest, SubstrCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = 37;
    int32_t strLen = str.length();

    vector<std::string> inputStr = { str, str, str, str, str, str, str, str };
    vector<int32_t> inputLen = { strLen, strLen, strLen, strLen, strLen, strLen, strLen, strLen };
    vector<int32_t> startIndexs = { 1, 1, 10, -5, 0, 37, -38, -37 };
    vector<int32_t> length = { 37, 5, 10, 7, 0, strLen + 5, 10, 37 };
    int32_t rowCnt = inputStr.size();
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> outResult(rowCnt);
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false, false};
    BatchSubstrChar(contextPtr, strAddr.data(), width, inputLen.data(), startIndexs.data(), length.data(), isAnyNull,
        outResult.data(), outLen.data(), rowCnt);

    vector<std::string> expected = { str, "时欧基乌斯", "hello! 回复哦", "色的圣诞袜", "", "袜", "", str };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    delete context;
}

TEST(BatchFunctionTest, SubstrWithStartZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());

    vector<std::string> inputStr = { str, str, str, str, str, str, str };
    vector<int32_t> inputLen = { strLen, strLen, strLen, strLen, strLen, strLen, strLen };
    vector<int32_t> startIndexs = { 1, 9, -3, 0, 37, -38, -37 };
    int32_t rowCnt = inputStr.size();
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> outResult(rowCnt);
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false};
    BatchSubstrWithStart(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), isAnyNull, outResult.data(),
        outLen.data(), rowCnt);

    vector<std::string> expected = { str, " hello! 回复哦黑色的and magic粉色的圣诞袜", "圣诞袜", "", "袜", "", str };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    delete context;
}

TEST(BatchFunctionTest, SubstrWithZhForSpark)
{
    std::string engineType("Spark");
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 h";
    int32_t strLen = static_cast<int32_t>(str.length());

    vector<std::string> inputStr = { str };
    vector<int32_t> inputLen = { strLen };
    vector<int32_t> outLen(inputStr.size());
    vector<uint8_t *> outResult(inputStr.size());
    vector<int32_t> startIndexs = { -15 };
    int32_t rowCnt = inputStr.size();
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false};
    BatchSubstrWithStart(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), isAnyNull, outResult.data(),
        outLen.data(), rowCnt);

    vector<std::string> expected = { str };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    inputStr = { str, str, str, str };
    rowCnt = inputStr.size();
    inputLen = { strLen, strLen, strLen, strLen };
    outLen.clear();
    outLen.reserve(rowCnt);
    outResult.clear();
    outResult.reserve(rowCnt);
    startIndexs = { -15, -15, -15, -15 };
    vector<int32_t> length = { 5, 6, 14, 20 };

    strAddr.clear();
    strAddr.reserve(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull1[] = {false, false, false, false};
    BatchSubstr(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), length.data(), isAnyNull1,
                outResult.data(), outLen.data(), rowCnt);
    expected = { "", "时", "时欧基乌斯侧后解 ", "时欧基乌斯侧后解 h" };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    engineType = "OLK";
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    delete context;
}

TEST(BatchFunctionTest, SubstrCharWithStartZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t strLen = str.length();

    vector<std::string> inputStr = { str, str, str, str, str, str, str };
    vector<int32_t> inputLen = { strLen, strLen, strLen, strLen, strLen, strLen, strLen };
    vector<int32_t> outLen(inputStr.size());
    vector<uint8_t *> outResult(inputStr.size());
    vector<int32_t> startIndexs = { 1, 9, -3, 0, 37, -38, -37 };
    int32_t rowCnt = inputStr.size();
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false};
    BatchSubstrCharWithStart(contextPtr, strAddr.data(), width, inputLen.data(), startIndexs.data(), isAnyNull,
                             outResult.data(), outLen.data(), rowCnt);

    vector<std::string> expected = { str, " hello! 回复哦黑色的and magic粉色的圣诞袜", "圣诞袜", "", "袜", "", str };
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(outResult[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }

    delete context;
}

TEST(BatchFunctionTest, LengthStrZh)
{
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = str.length();

    vector<std::string> inputStr = { str, "解 hello! 回复哦黑色的" };
    vector<int32_t> inputLen = { strLen, 29 };
    vector<int64_t> outLen(inputStr.size());
    int32_t rowCnt = inputStr.size();
    vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false};
    BatchLengthStr(strAddr.data(), inputLen.data(), isAnyNull, outLen.data(), rowCnt);
    vector<int64_t> expected = { 37, 15 };
    for (int32_t i = 0; i < rowCnt; i++) {
        EXPECT_EQ(outLen[i], expected[i]);
    }
}


TEST(BatchFunctionTest, LikeStrZh)
{
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = str.length();

    vector<std::string> inputStr = { str, str, str, str };
    vector<int32_t> inputLen = { strLen, strLen, strLen, strLen };
    vector<std::string> patternStr = { "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$",
        "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞..$",
        "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣.*$",
        "^欧时基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.*$" };
    int32_t rowCnt = inputStr.size();
    bool output[rowCnt];
    vector<uint8_t *> strAddr(rowCnt);
    vector<uint8_t *> patternAddr(rowCnt);
    vector<int32_t> patternLen(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
        patternAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(patternStr[i].c_str()));
        patternLen[i] = static_cast<int32_t>(patternStr[i].length());
    }

    bool isAnyNull[] = {false, false, false, false};
    BatchLikeStr(strAddr.data(), inputLen.data(), patternAddr.data(), patternLen.data(), isAnyNull, output, rowCnt);

    std::vector<bool> expected = { true, false, true, false };
    for (int32_t i = 0; i < rowCnt; i++) {
        EXPECT_EQ(output[i], expected[i]);
    }
}

TEST(BatchFunctionTest, LikeCharZh)
{
    vector<std::string> inputStr = { "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜", "时欧基乌",
        "时欧基乌", "时欧基乌" };
    vector<std::string> patternStr = { "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$", "^时欧基乌..$",
        "^时欧基乌.$", "^时欧基乌.*$" };
    int32_t batch = 4;
    vector<bool> expected = { true, true, false, true };
    vector<int32_t> width = { 37, 6, 6, 6 };
    for (int32_t i = 0; i < batch; i++) {
        int32_t rowCnt = 1;
        vector<int32_t> inputLen = { static_cast<int32_t>(inputStr[i].length()) };
        bool output[rowCnt];
        vector<uint8_t *> strAddr = { reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str())) };
        vector<uint8_t *> patternAddr = { reinterpret_cast<uint8_t *>(const_cast<char *>(patternStr[i].c_str())) };
        vector<int32_t> patternLen = { static_cast<int32_t>(patternStr[i].length()) };

        bool isAnyNull[] = {false};
        BatchLikeChar(strAddr.data(), width[i], inputLen.data(), patternAddr.data(), patternLen.data(),
                      isAnyNull, output, rowCnt);
        EXPECT_EQ(output[0], expected[i]);
    }
}

TEST(BatchFunctionTest, ReplaceStrStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> str = {
        "apple", "粉色的圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜", "", "粉色de圣诞袜"
    };
    vector<int32_t> strLen = { 5, 18, 17, 17, 17, 0, 17 };
    vector<string> searchStr = { "", "", "", "", "de圣", "", "" };
    vector<int32_t> searchLen = { 0, 0, 0, 0, 5, 0, 0 };
    vector<string> replaceStr = { "*w*", "*w*", "*w*", "*的*", "*的*", "", "" };
    vector<int32_t> replaceLen = { 3, 3, 3, 5, 5, 0, 0 };
    int32_t rowCnt = str.size();
    vector<uint8_t *> output(rowCnt);
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> strAddr(rowCnt);
    vector<uint8_t *> searchStrAddr(rowCnt);
    vector<uint8_t *> replaceStrAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(str[i].c_str()));
        searchStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(searchStr[i].c_str()));
        replaceStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(replaceStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false };
    BatchReplaceStrStrStrWithRep(contextPtr, strAddr.data(), strLen.data(), searchStrAddr.data(), searchLen.data(),
        replaceStrAddr.data(), replaceLen.data(), isAnyNull, output.data(), outLen.data(), rowCnt);
    vector<string> expected = { "*w*a*w*p*w*p*w*l*w*e*w*",
        "*w*粉*w*色*w*的*w*圣*w*诞*w*袜*w*",
        "*w*粉*w*色*w*d*w*e*w*圣*w*诞*w*袜*w*",
        "*的*粉*的*色*的*d*的*e*的*圣*的*诞*的*袜*的*",
        "粉色*的*诞袜",
        "",
        "粉色de圣诞袜" };
    for (int32_t i = 0; i < rowCnt; i++) {
        string actual(reinterpret_cast<char *>(output[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }
    delete context;
}

TEST(BatchFunctionTest, ReplaceWithoutRepZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> str = { "apple", "apple", "粉色的圣诞袜", "粉色的圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜" };
    vector<int32_t> strLen = { 5, 5, 18, 18, 17, 17 };
    vector<string> searchStr = { "", "pp", "", "圣诞", "", "色de" };
    vector<int32_t> searchLen = { 0, 2, 0, 6, 0, 5 };
    int32_t rowCnt = str.size();
    vector<uint8_t *> output(rowCnt);
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> strAddr(rowCnt);
    vector<uint8_t *> searchStrAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(str[i].c_str()));
        searchStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(searchStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false};
    BatchReplaceStrStrWithoutRep(contextPtr, strAddr.data(), strLen.data(), searchStrAddr.data(), searchLen.data(),
        isAnyNull, output.data(), outLen.data(), rowCnt);
    vector<string> expected = {
        "apple", "ale", "粉色的圣诞袜", "粉色的袜", "粉色de圣诞袜", "粉圣诞袜",
    };
    for (int32_t i = 0; i < rowCnt; i++) {
        string actual(reinterpret_cast<char *>(output[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> ap = { "你是Chinese?", "", "pink圣诞袜" };
    vector<int32_t> apLen = { 14, 0, 13 };
    vector<string> bp = { "Yes我是", "粉色de圣诞袜", "" };
    vector<int32_t> bpLen = { 9, 17, 0 };
    int32_t rowCnt = ap.size();
    vector<uint8_t *> output(rowCnt);
    vector<int32_t> outLen(rowCnt);
    vector<uint8_t *> apAddr(rowCnt);
    vector<uint8_t *> bpAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        apAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[i].c_str()));
        bpAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[i].c_str()));
    }
    bool isAnyNull[] = {false, false, false};
    BatchConcatStrStr(contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bpLen.data(), isAnyNull, output.data(),
        outLen.data(), rowCnt);
    vector<string> expected = { "你是Chinese?Yes我是", "粉色de圣诞袜", "pink圣诞袜" };
    for (int32_t i = 0; i < rowCnt; i++) {
        string actual(reinterpret_cast<char *>(output[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatCharCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> ap = { "粉色de圣诞袜", "*黑色*",      "Hei你好吗",    "Oh我很好",
        "Hei你好吗   ", "Oh我很好  ",  "   Hei你好吗", "   Oh我很好",
        "Hei   你好吗", "Oh   我很好", "   ",          "Oh我很好",
        "Hei你好吗",    "   ",         "Hei你好吗",    "" };
    vector<int32_t> aWidth = { 7, 8, 10, 12, 12, 5, 8, 8 };
    vector<string> bp = { "*黑色*",      "粉色de",      "Oh我很好",     "Hei你好吗",   "Oh我很好  ",
        "Hei你好吗  ", "   Oh我很好", "   Hei你好吗", "Oh   我很好", "Hei   你好",
        "Oh我很好   ", "   ",         "   ",          "Hei你好吗",   "",
        "Hei你好" };
    vector<int32_t> bWidth = { 4, 8, 8, 12, 8, 12, 5, 5 };
    vector<string> expected = { "粉色de圣诞袜*黑色*",
        "*黑色*   粉色de",
        "Hei你好吗  Oh我很好",
        "Oh我很好   Hei你好吗",
        "Hei你好吗    Oh我很好  ",
        "Oh我很好     Hei你好吗  ",
        "   Hei你好吗      Oh我很好",
        "   Oh我很好       Hei你好吗",
        "Hei   你好吗   Oh   我很好",
        "Oh   我很好    Hei   你好",
        "     Oh我很好   ",
        "Oh我很好   ",
        "Hei你好吗     ",
        "        Hei你好吗",
        "Hei你好吗",
        "        Hei你好" };
    int32_t batch = 8;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        vector<uint8_t *> output(rowCnt);
        vector<int32_t> outLen(rowCnt);
        vector<int32_t> apLen(rowCnt);
        vector<int32_t> bpLen(rowCnt);
        vector<uint8_t *> apAddr(rowCnt);
        vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }
        bool isAnyNull[] = {false, false};
        BatchConcatCharChar(contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(), bWidth[i], bpLen.data(),
            isAnyNull, output.data(), outLen.data(), rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            string actual(reinterpret_cast<char *>(output[row]), outLen[row]);
            EXPECT_EQ(actual, expected[i * rowCnt + row]);
        }
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatCharStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> ap = { "*你是谁呢*", "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "" };
    vector<int32_t> aWidth = { 6, 10, 10 };
    vector<string> bp = { "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "", "*你是谁呢*" };
    vector<string> expected = { "*你是谁呢*我很OK",       "我很OK  *你是谁呢*", "*你是谁呢*    我很OK",
        "我很OK      *你是谁呢*", "*你是谁呢*",         "          *你是谁呢*" };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        vector<uint8_t *> output(rowCnt);
        vector<int32_t> outLen(rowCnt);
        vector<int32_t> apLen(rowCnt);
        vector<int32_t> bpLen(rowCnt);
        vector<uint8_t *> apAddr(rowCnt);
        vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isAnyNull[] = {false, false};
        BatchConcatCharStr(contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(), bpLen.data(),
            isAnyNull, output.data(), outLen.data(), rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            string actual(reinterpret_cast<char *>(output[row]), outLen[row]);
            EXPECT_EQ(actual, expected[i * rowCnt + row]);
        }
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatStrCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    vector<string> ap = { "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜   ", "" };
    vector<string> bp = { "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "", "粉色de圣诞袜   " };
    vector<int32_t> bWidth = { 4, 6, 5 };
    vector<string> expected = { "粉色de圣诞袜*黑色*", "*黑色*粉色de圣诞袜", "粉色de圣诞袜*黑色*",
        "*黑色*粉色de圣诞袜", "粉色de圣诞袜   ",    "粉色de圣诞袜   " };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        vector<uint8_t *> output(rowCnt);
        vector<int32_t> outLen(rowCnt);
        vector<int32_t> apLen(rowCnt);
        vector<int32_t> bpLen(rowCnt);
        vector<uint8_t *> apAddr(rowCnt);
        vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isAnyNull[] = {false, false};
        BatchConcatStrChar(contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bWidth[i], bpLen.data(),
            isAnyNull, output.data(), outLen.data(), rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            string actual(reinterpret_cast<char *>(output[row]), outLen[row]);
            EXPECT_EQ(actual, expected[i * rowCnt + row]);
        }
    }
    delete context;
}
}