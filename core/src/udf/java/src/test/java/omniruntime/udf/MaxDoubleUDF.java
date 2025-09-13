/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import static java.lang.Math.max;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * MaxDoubleUDF for test
 *
 * @since 2022-8-3
 */
public class MaxDoubleUDF extends UDF {
    /**
     * Calculates the max double value for test.
     *
     * @param paramA the first param
     * @param paramB the second param
     * @return return the max of paramA and paramB
     */
    public Double evaluate(Double paramA, Double paramB) {
        if (paramA == null || paramB == null) {
            return null;
        }
        return max(paramA, paramB);
    }
}
