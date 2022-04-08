/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Join type.
 *
 * @since 2021-06-30
 */
public class JoinType extends Constant {
    /**
     * The constant OMNI_JOIN_TYPE_INNER.
     */
    public static JoinType OMNI_JOIN_TYPE_INNER;

    /**
     * The constant OMNI_JOIN_TYPE_LEFT.
     */
    public static JoinType OMNI_JOIN_TYPE_LEFT;

    /**
     * The constant OMNI_JOIN_TYPE_RIGHT.
     */
    public static JoinType OMNI_JOIN_TYPE_RIGHT;

    /**
     * The constant OMNI_JOIN_TYPE_FULL.
     */
    public static JoinType OMNI_JOIN_TYPE_FULL;

    private static final long serialVersionUID = -4086671645951741450L;

    /**
     * Instantiates a new Join type.
     *
     * @param value the value
     */
    @JsonCreator
    public JoinType(@JsonProperty("value") int value) {
        super(value);
    }
}
