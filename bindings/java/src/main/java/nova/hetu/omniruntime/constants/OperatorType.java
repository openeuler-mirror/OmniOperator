/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * OperatorType
 *
 * @since 2022-12-19
 */
public class OperatorType extends Constant {
    /**
     * The constant OMNI_FILTER_AND_PROJECT.
     */
    public static OperatorType OMNI_FILTER_AND_PROJECT;

    /**
     * The constant OMNI_PROJECT.
     */
    public static OperatorType OMNI_PROJECT;

    /**
     * The constant OMNI_LIMIT.
     */
    public static OperatorType OMNI_LIMIT;

    /**
     * The constant OMNI_DISTINCT_LIMIT.
     */
    public static OperatorType OMNI_DISTINCT_LIMIT;

    /**
     * The constant OMNI_SORT.
     */
    public static OperatorType OMNI_SORT;

    /**
     * The constant OMNI_TOPN.
     */
    public static OperatorType OMNI_TOPN;

    /**
     * The constant OMNI_AGGREGATION.
     */
    public static OperatorType OMNI_AGGREGATION;

    /**
     * The constant OMNI_HASH_AGGREGATION.
     */
    public static OperatorType OMNI_HASH_AGGREGATION;

    /**
     * The constant OMNI_WINDOW.
     */
    public static OperatorType OMNI_WINDOW;

    /**
     * The constant OMNI_HASH_BUILDER.
     */
    public static OperatorType OMNI_HASH_BUILDER;

    /**
     * The constant OMNI_LOOKUP_JOIN.
     */
    public static OperatorType OMNI_LOOKUP_JOIN;

    /**
     * The constant OMNI_LOOKUP_OUTER_JOIN.
     */
    public static OperatorType OMNI_LOOKUP_OUTER_JOIN;

    /**
     * The constant OMNI_SMJ_BUFFER.
     */
    public static OperatorType OMNI_SMJ_BUFFER;

    /**
     * The constant OMNI_SMJ_STREAM.
     */
    public static OperatorType OMNI_SMJ_STREAM;

    /**
     * The constant OMNI_PARTITIONED_OUTPUT.
     */
    public static OperatorType OMNI_PARTITIONED_OUTPUT;

    /**
     * The constant OMNI_UNION.
     */
    public static OperatorType OMNI_UNION;

    /**
     * The constant OMNI_FUSION.
     */
    public static OperatorType OMNI_FUSION;

    private static final long serialVersionUID = -4350951859220483149L;

    /**
     * Instantiates a new operator type.
     *
     * @param value the value
     */
    @JsonCreator
    public OperatorType(@JsonProperty("value") int value) {
        super(value);
    }
}
