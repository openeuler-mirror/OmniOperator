/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Type Verification
 */

#ifndef OMNI_RUNTIME_TYPE_UTILS_H
#define OMNI_RUNTIME_TYPE_UTILS_H
#include <string_view>
namespace omniruntime::vec {

    /**
     * Marker class for no encoding
     * @tparam T
     */
    template<typename T>
    struct no_encoding {
    };

    /**
     * The helper to resolve the type used in SetValue and GetValue to avoid memory copy
     * When the RAW DATA TYPE is of arithematic data type such as int, float, dobule, we will use the data type as is
     * When the RAW DATA TYPE is an object such as string, struct, map, etc., we will use reference instead to avoid
     * memory copy.
     *
     * @tparam RAW_DATA_TYPE
     */
    template<typename RAW_DATA_TYPE>
    struct PARAM_TYPE {
        using type = RAW_DATA_TYPE;
    };

    /**
     * The helper to find the correct pointer type for the values.
     * For raw data types such as arithematic types int, float, double, etc, we return values will be of type: int*,
     * float*, double*, etc.
     * When encoding is using, we will need a reference to the Object encoding the whole array, e.g.
     * string (RAW DATA TYPE) with dictionary array (DictionaryArray) encoding will be of type DictionaryArray<string>
     *
     * A proper type for values will enable SetValue and GetValue to use [] operators
     *
     * @tparam RAW_DATA_TYPE
     * @tparam ENC
     * @return returns the correct data type for the values pointer
     */
    template<typename RAW_DATA_TYPE, template<typename> class ENC>
    struct VALUE_TYPE {
        using type = std::conditional_t<
                std::is_same_v<no_encoding < RAW_DATA_TYPE>, ENC<RAW_DATA_TYPE>>,
        RAW_DATA_TYPE *, ENC<RAW_DATA_TYPE>>;
    };

    /**
     * determine whether a certain type is a container storage, like std::string_view, and
     * a general type such as int/long is an array storage
     * @tparam RAW_DATA_TYPE
     */
    template <typename RAW_DATA_TYPE>
    inline constexpr bool is_container_v = std::is_same_v<RAW_DATA_TYPE, std::string_view>;
}

#endif //OMNI_RUNTIME_TYPE_UTILS_H
