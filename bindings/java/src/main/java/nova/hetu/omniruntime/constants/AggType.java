/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type Agg type.
 *
 * @since 20210630
 */
@SuppressWarnings("StaticVariableName")
public class AggType extends Constant {
    /**
     * The constant OMNI_AGGREGATION_TYPE_SUM.
     */
    public static AggType OMNI_AGGREGATION_TYPE_SUM;

    /**
     * The constant OMNI_AGGREGATION_TYPE_COUNT.
     */
    public static AggType OMNI_AGGREGATION_TYPE_COUNT;

    /**
     * The constant OMNI_AGGREGATION_TYPE_AVG.
     */
    public static AggType OMNI_AGGREGATION_TYPE_AVG;

    /**
     * The constant OMNI_AGGREGATION_TYPE_MAX.
     */
    public static AggType OMNI_AGGREGATION_TYPE_MAX;

    /**
     * The constant OMNI_AGGREGATION_TYPE_MIN.
     */
    public static AggType OMNI_AGGREGATION_TYPE_MIN;

    /**
     * The constant OMNI_AGGREGATION_TYPE_DNV.
     */
    public static AggType OMNI_AGGREGATION_TYPE_DNV;

    /**
     * Instantiates a new Agg type.
     *
     * @param value the value
     */
    public AggType(int value) {
        super(value);
    }
}
