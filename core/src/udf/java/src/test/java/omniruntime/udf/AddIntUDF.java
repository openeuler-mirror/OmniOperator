/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import static java.lang.Math.addExact;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * AddIntUDF for test
 *
 * @since 2022-8-3
 */
public class AddIntUDF extends UDF {
    /**
     * Calculates the sum of two int datas for test.
     *
     * @param paramA the first param
     * @param paramB the second param
     * @return return the sum of paramA and paramA
     */
    public Integer evaluate(Integer paramA, Integer paramB) {
        if (paramA == null || paramB == null) {
            return null;
        }
        return addExact(paramA, paramB);
    }
}
