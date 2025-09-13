/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

/**
 * Vec util
 *
 * @since 2021-12-17
 */
public class VecUtil {
    private VecUtil() {

    }

    /**
     * Set data type
     *
     * @param vec vec
     * @param dataType data type
     */
    public static void setDataType(Vec vec, DataType dataType) {
        vec.setDataType(dataType);
    }
}
