/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * AndBooleanUDF for test
 *
 * @since 2022-8-3
 */
public class AndBooleanUDF extends UDF {
    /**
     * Calculates the result of and two boolean datas for test.
     *
     * @param paramA the first param
     * @param paramB the second param
     * @return return the and result of paramA and paramB
     */
    public Boolean evaluate(Boolean paramA, Boolean paramB) {
        if (paramA == null || paramB == null) {
            return null;
        }
        return paramA && paramB;
    }
}
