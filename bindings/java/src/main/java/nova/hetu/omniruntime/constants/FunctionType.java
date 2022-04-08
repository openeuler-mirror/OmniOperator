/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Agg type.
 *
 * @since 2021-06-30
 */
public class FunctionType extends Constant {
    /**
     * The constant OMNI_AGGREGATION_TYPE_SUM.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_SUM;

    /**
     * The constant OMNI_AGGREGATION_TYPE_COUNT_COLUMN.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_COUNT_COLUMN;

    /**
     * The constant OMNI_AGGREGATION_TYPE_COUNT_ALL.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_COUNT_ALL;

    /**
     * The constant OMNI_AGGREGATION_TYPE_AVG.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_AVG;

    /**
     * The constant OMNI_AGGREGATION_TYPE_MAX.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_MAX;

    /**
     * The constant OMNI_AGGREGATION_TYPE_MIN.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_MIN;

    /**
     * The constant OMNI_AGGREGATION_TYPE_DNV.
     */
    public static FunctionType OMNI_AGGREGATION_TYPE_DNV;

    /**
     * The constant OMNI_WINDOW_TYPE_ROW_NUMBER.
     */
    public static FunctionType OMNI_WINDOW_TYPE_ROW_NUMBER;

    /**
     * The constant OMNI_WINDOW_TYPE_RANK.
     */
    public static FunctionType OMNI_WINDOW_TYPE_RANK;

    private static final long serialVersionUID = 5337378607473315604L;

    /**
     * Instantiates a new Agg type.
     *
     * @param value the value
     */
    @JsonCreator
    public FunctionType(@JsonProperty("value") int value) {
        super(value);
    }
}
