/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

public class VecUtil {
    private VecUtil() {

    }

    public static void setDataType(Vec vec, DataType dataType) {
        vec.setDataType(dataType);
    }
}
