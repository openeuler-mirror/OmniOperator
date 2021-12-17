/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;

public class VecUtil {
    private VecUtil() {

    }

    public static void setType(Vec vec, VecType vecType) {
        vec.setType(vecType);
    }
}
