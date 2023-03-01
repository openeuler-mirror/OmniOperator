#include "gtest/gtest.h"
#include "vector/vector.h"
#include "vector/dictionary_container.h"
#include "test.h"
#include "vector/vector_helper.h"
//
// Created by caicongzhi on 2022-11-01.
//

namespace omniruntime::vec::test {

    template<typename CONTAINER>
    void string_vector_get_set_value() {
        int vectorSize = 1000;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;

        std::string valuePrefix;
        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            valuePrefix = "hello__";
            stringWidth = OMNI_SMALL_WIDTH;
        } else {
            valuePrefix = "hello_world__";
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        for (int i = 0; i < vectorSize; i++) {
            std::string value = valuePrefix + std::to_string(i);
            std::string_view input(value.data(), value.size());
            vector->SetValue(i, input);
        }

        for (int i = 0; i < vectorSize; i++) {
            std::string value = valuePrefix + std::to_string(i);
            std::string_view output = vector->GetValue(i);
            EXPECT_EQ(value, output);
        }
    }

    template<typename CONTAINER>
    void string_vector_get_set_empty_value() {
        int vectorSize = 1000;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;

        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            stringWidth = OMNI_SMALL_WIDTH;
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        std::string empty = "";
        for (int i = 0; i < vectorSize; i++) {
            std::string_view input(empty.data(), 0);
            vector->SetValue(i, input);
        }

        for (int i = 0; i < vectorSize; i++) {
            std::string_view output = vector->GetValue(i);
            EXPECT_EQ(output.size(), 0);
        }
    }

    template<typename CONTAINER>
    void string_vector_size_invalid() {
        int vectorSize = -1;
        EXPECT_ANY_THROW(auto container = std::make_shared<CONTAINER>(vectorSize));
    }

    template<typename CONTAINER>
    void string_vector_append_value() {
        int vectorSize = 5;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;

        std::string valuePrefix;
        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            valuePrefix = "hello__";
            stringWidth = OMNI_SMALL_WIDTH;
        } else {
            valuePrefix = "hello_world__";
        }

        auto v1BaseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *v1 = (Vector<CONTAINER> *)v1BaseVector.get();

        std::vector<std::string> expected;
        for (int32_t i = 0; i < vectorSize; i++) {
            std::string value = valuePrefix + std::to_string(i);
            std::string_view input(value.data(), value.size());
            v1->SetValue(i, input);
            expected.push_back(value);
        }

        int32_t appendedVecSize = 15;
        auto appendedBaseVector = VectorHelper::CreateStringVector(appendedVecSize, stringWidth);
        auto *appended = (Vector<CONTAINER> *)appendedBaseVector.get();
        appended->Append(v1, 0, vectorSize);

        auto v2BaseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *v2WithNull = (Vector<CONTAINER> *)v2BaseVector.get();
        for (int32_t i = 0; i < vectorSize; i++) {
            if (i % 2 == 0) {
                v2WithNull->SetNull(i);
                continue;
            }
            std::string_view input(expected[i].data(), expected[i].size());
            v2WithNull->SetValue(i, input);
        }
        appended->Append(v2WithNull, 5, 5);

        auto v3BaseVector = VectorHelper::CreateStringVector(0, stringWidth);
        auto *v3Emtpy = (Vector<CONTAINER> *)v3BaseVector.get();
        appended->Append(v3Emtpy, 10, 0);

        auto v4BaseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *v4OverBounds = (Vector<CONTAINER> *)v4BaseVector.get();
        for (int32_t i = 0; i < vectorSize; i++) {
            std::string_view input(expected[i].data(), expected[i].size());
            v4OverBounds->SetValue(i, input);
        }
        appended->Append(v4OverBounds, 10, vectorSize + 1);

        std::vector<bool> expectedNull {false, false, false, false, false, true, false, true, false, true};
        for (int32_t i = 0; i < appendedVecSize; i++) {
            // append empty vector or beyond the bound
            if (i >= 10) {
                EXPECT_FALSE(appended->IsNull(i));
                continue;
            }
            // append success for value check
            if (appended->IsNull(i)) {
                EXPECT_EQ(expectedNull[i], appended->IsNull(i));
                continue;
            }
            EXPECT_EQ(expected[i % 5], appended->GetValue(i));
        }
    }

    template<typename CONTAINER>
    void string_vector_copy_positions() {
        int vectorSize = 10;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;
        std::string valuePrefix;

        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            valuePrefix = "hello__";
            stringWidth = OMNI_SMALL_WIDTH;
        } else {
            valuePrefix = "hello_world__";
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        for (int i = 0; i < vectorSize; i++) {
            if (i % 2 == 0) {
                vector->SetNull(i);
            } else {
                std::string value = valuePrefix + std::to_string(i);
                std::string_view input(value.data(), value.size());
                vector->SetValue(i, input);
            }
        }

        int index[] = {2, 3, 4, 5, 6, 7};
        int offset1 = 0;
        int offset2 = 1;
        int copySize = 4;
        auto vectorOffsetZero = vector->CopyPositions(index, offset1, copySize);
        auto vectorOffsetNotZero = vector->CopyPositions(index, offset2, copySize);

        for (int32_t i = 0; i < copySize; i++) {
            if (i % 2 == 0) {
                EXPECT_EQ(vectorOffsetZero->IsNull(i), true);
                std::string_view originValue = vector->GetValue(index[i + offset2]);
                std::string_view offsetNotZeroValue = vectorOffsetNotZero->GetValue(i);
                EXPECT_EQ(offsetNotZeroValue, originValue);
                continue;
            }
            std::string_view originValue = vector->GetValue(index[i + offset1]);
            std::string_view offsetZeroValue = vectorOffsetZero->GetValue(i);
            EXPECT_EQ(vectorOffsetNotZero->IsNull(i), true);
            EXPECT_EQ(offsetZeroValue, originValue);
        }

        auto vectorEmpty = vector->CopyPositions(index, offset2, 0);
        EXPECT_EQ(vectorEmpty->GetSize(), 0);
        auto vectorNegtive = vector->CopyPositions(index, offset1, -1);
        EXPECT_EQ(vectorNegtive, nullptr);
    }


    template<typename CONTAINER>
    void string_vector_slice() {
        int vectorSize = 1000;
        int offset = 50;
        int len = 10;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;

        std::string valuePrefix;
        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            valuePrefix = "hello__";
            stringWidth = OMNI_SMALL_WIDTH;
        } else {
            valuePrefix = "hello_world__";
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *parent = (Vector<CONTAINER> *)baseVector.get();

        for (int i = 0; i < vectorSize; i++) {
            std::string value = valuePrefix + std::to_string(i);
            std::string_view input(value.data(), value.size());
            parent->SetValue(i, input);
        }

        auto vector = parent->Slice(offset, len);

        // WARNING:
        // setting value on encoded vector not supported
        // the following code will cause compilation error
        //vector->SetValue(1, vector->GetValue(2));

        for (int i = 0; i < len; i++) {
            std::string value = valuePrefix + std::to_string(i + offset);
            auto output = vector->GetValue(i);
            EXPECT_EQ(value, output);
        }
    }

    template<typename CONTAINER>
    void string_vector_get_encoding() {
        int vectorSize = 1000;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;
        StringEncoding expect = OMNI_INVALID_STRING;

        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            stringWidth = OMNI_SMALL_WIDTH;
            expect = OMNI_SMALL_STRING;
        } else {
            expect = OMNI_LARGE_STRING;
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        EXPECT_EQ(vector->GetStringEncoding(), expect);
        EXPECT_EQ(baseVector->GetStringEncoding(), expect);
    }

    template<typename CONTAINER>
    void string_vector_get_used_bytes() {
        int vectorSize = 100000;
        uint32_t stringWidth = OMNI_LARGE_WIDTH;

        std::string valuePrefix;
        if constexpr(std::is_same_v<SmallStringContainer<std::string_view>, CONTAINER>) {
            valuePrefix = "hello__";
            stringWidth = OMNI_SMALL_WIDTH;
        } else {
            valuePrefix = "hello_world__";
        }

        auto baseVector = VectorHelper::CreateStringVector(vectorSize, stringWidth);
        auto *vector = (Vector<CONTAINER> *)baseVector.get();

        size_t expect = 0;
        for (int i = 0; i < vectorSize; i++) {
            std::string value = valuePrefix + std::to_string(i);
            std::string_view input(value.data(), value.size());
            vector->SetValue(i, input);
            expect += value.size() + 1;
        }

        size_t real = UnsafeStringContainer::GetCapacityInBytes(
                unsafe::UnsafeStringVector::GetContainer(vector).get());
        EXPECT_LE(expect, real);
    }

    TEST(vector2, vector_get_set_value_small_string) {
        string_vector_get_set_value<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_get_set_value_large_string) {
        string_vector_get_set_value<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_get_set_value_small_empty_string) {
        string_vector_get_set_empty_value<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_get_set_value_large_empty_string) {
        string_vector_get_set_empty_value<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, small_string_vector_size_invalid) {
        string_vector_size_invalid<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, large_string_vector_size_invalid) {
        string_vector_size_invalid<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_append_small_string) {
        string_vector_append_value<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_append_large_string) {
        string_vector_append_value<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_copy_positions_small_string) {
        string_vector_copy_positions<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, vector_copy_positions_large_string) {
        string_vector_copy_positions<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, slice_container_small_string) {
        string_vector_slice<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, slice_container_large_string) {
        string_vector_slice<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, string_vector_get_small_encoding) {
        string_vector_get_encoding<SmallStringContainer<std::string_view>>();
    }

    TEST(vector2, string_vector_get_large_encoding) {
        string_vector_get_encoding<LargeStringContainer<std::string_view>>();
    }

    TEST(vector2, string_vector_get_large_size) {
        string_vector_get_used_bytes<LargeStringContainer<std::string_view>>();
    }
}