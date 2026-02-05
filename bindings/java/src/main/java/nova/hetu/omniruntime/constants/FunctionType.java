/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type Agg type.
 *
 * @since 2021-06-30
 */
public class FunctionType extends Constant {
    /**
     * The constant OMNI_AGGREGATION_TYPE_SUM.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_SUM = new FunctionType(0);

    /**
     * The constant OMNI_AGGREGATION_TYPE_COUNT_COLUMN.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_COUNT_COLUMN = new FunctionType(1);

    /**
     * The constant OMNI_AGGREGATION_TYPE_COUNT_ALL.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_COUNT_ALL = new FunctionType(2);

    /**
     * The constant OMNI_AGGREGATION_TYPE_AVG.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_AVG = new FunctionType(3);

    /**
     * The constant OMNI_AGGREGATION_TYPE_SAMP.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_SAMP = new FunctionType(4);

    /**
     * The constant OMNI_AGGREGATION_TYPE_MAX.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_MAX = new FunctionType(5);

    /**
     * The constant OMNI_AGGREGATION_TYPE_MIN.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_MIN = new FunctionType(6);

    /**
     * The constant OMNI_AGGREGATION_TYPE_DNV.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_DNV = new FunctionType(7);

    /**
     * The constant OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL = new FunctionType(8);

    /**
     * The constant OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL = new FunctionType(9);

    /**
     * The constant OMNI_AGGREGATION_TYPE_INVALID.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_INVALID = new FunctionType(10);

    /**
     * The constant OMNI_WINDOW_TYPE_ROW_NUMBER.
     */
    public static final FunctionType OMNI_WINDOW_TYPE_ROW_NUMBER = new FunctionType(11);

    /**
     * The constant OMNI_WINDOW_TYPE_RANK.
     */
    public static final FunctionType OMNI_WINDOW_TYPE_RANK = new FunctionType(12);

    /**
     * The constant OMNI_WINDOW_TYPE_PERCENT_RANK.
     */
    public static final FunctionType OMNI_WINDOW_TYPE_PERCENT_RANK = new FunctionType(13);

    /**
     * The constant OMNI_AGGREGATION_TYPE_TRY_SUM.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_TRY_SUM = new FunctionType(14);

    /**
     * The constant OMNI_AGGREGATION_TYPE_TRY_AVG.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_TRY_AVG = new FunctionType(15);

    /**
     * The constant OMNI_AGGREGATION_TYPE_BLOOM_FILTER.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_BLOOM_FILTER = new FunctionType(16);

    /**
     * The constant OMNI_AGGREGATION_TYPE_MIN_BY.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_MIN_BY = new FunctionType(17);

    /**
     * The constant OMNI_AGGREGATION_TYPE_MAX_BY.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_MAX_BY = new FunctionType(18);

    /**
     * The constant OMNI_AGGREGATION_TYPE_BIT_AND.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_BIT_AND = new FunctionType(19);

    /**
     * The constant OMNI_AGGREGATION_TYPE_BIT_OR.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_BIT_OR = new FunctionType(20);

    /**
     * The constant OMNI_AGGREGATION_TYPE_BIT_XOR.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_BIT_XOR = new FunctionType(21);

    /**
     * The constant OMNI_AGGREGATION_TYPE_STD_POP.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_STD_POP = new FunctionType(22);

    /**
     * The constant OMNI_AGGREGATION_TYPE_VAR_POP.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_VAR_POP = new FunctionType(23);

    /**
     * The constant OMNI_AGGREGATION_TYPE_VAR_SAMP.
     */
    public static final FunctionType OMNI_AGGREGATION_TYPE_VAR_SAMP = new FunctionType(24);

    private static final long serialVersionUID = 5337378607473315604L;

    /**
     * Instantiates a new Agg type.
     *
     * @param value the value
     */
    public FunctionType(int value) {
        super(value);
    }
}
