/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

/**
 * vec encoding.
 *
 * @since 2022-02-17
 */
public enum VecEncoding {
    OMNI_VEC_ENCODING_FLAT,
    OMNI_VEC_ENCODING_DICTIONARY,
    OMNI_VEC_ENCODING_CONTAINER,
    OMNI_VEC_ENCODING_LAZY,
    OMNI_VEC_ENCODING_INVALID
}
