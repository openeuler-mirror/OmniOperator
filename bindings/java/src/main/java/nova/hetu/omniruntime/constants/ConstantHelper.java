/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import java.util.Arrays;

/**
 * The type Constant helper.
 *
 * @since 2021-06-30
 */
public class ConstantHelper {
    private ConstantHelper() {
    }

    /**
     * To native constants int [ ].
     *
     * @param constants the constants
     * @return the int [ ]
     */
    public static int[] toNativeConstants(Constant[] constants) {
        return Arrays.stream(constants).map(Constant::getValue).mapToInt(Integer::intValue).toArray();
    }
}
