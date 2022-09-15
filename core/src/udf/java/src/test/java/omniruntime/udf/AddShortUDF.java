/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import nova.hetu.omniruntime.utils.OmniRuntimeException;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * AddShortUDF for test
 *
 * @since 2022-8-3
 */
public class AddShortUDF extends UDF {
    /**
     * Calculates the sum of two short datas for test.
     *
     * @param paramA the first param
     * @param paramB the second param
     * @return return the sum of paramA and paramB
     */
    public Short evaluate(Short paramA, Short paramB) {
        if (paramA == null || paramB == null) {
            return null;
        }
        short sum = (short) (paramA + paramB);
        if (((paramA ^ sum) & (paramB ^ sum)) < 0) {
            throw new OmniRuntimeException("short overflow");
        } else {
            return sum;
        }
    }
}
