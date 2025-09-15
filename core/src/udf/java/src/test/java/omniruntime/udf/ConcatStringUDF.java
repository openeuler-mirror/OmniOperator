/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ConcatStringUDF for test
 *
 * @since 2022-8-3
 */
public class ConcatStringUDF extends UDF {
    /**
     * Concat the two string datas for test.
     *
     * @param paramA the first param
     * @param paramB the second param
     * @return return the concat result of paramA and paramB
     */
    public String evaluate(String paramA, String paramB) {
        if (paramA == null || paramB == null) {
            return null;
        } else {
            return paramA + paramB;
        }
    }
}
