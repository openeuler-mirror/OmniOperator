/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import nova.hetu.omniruntime.constants.VecType;

/**
 * The type Vector helper.
 *
 * @since 20210630
 */
public class VectorHelper
{
    private VectorHelper()
    {
    }

    /**
     * Transform int to vec type vec type [ ].
     *
     * @param values the values
     * @return the vec type [ ]
     */
    public static VecType[] transformIntToVecType(int[] values)
    {
        VecType[] vecTypes = new VecType[values.length];
        for (int i = 0; i < values.length; ++i) {
            vecTypes[i] = new VecType(values[i]);
        }
        return vecTypes;
    }
}
