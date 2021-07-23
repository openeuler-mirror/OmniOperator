/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type Vec type.
 *
 * @since 20210630
 */
@SuppressWarnings("StaticVariableName")
public class VecType extends Constant {
    /**
     * The constant OMNI_VEC_TYPE_INT.
     */
    public static VecType OMNI_VEC_TYPE_INT;

    /**
     * The constant OMNI_VEC_TYPE_LONG.
     */
    public static VecType OMNI_VEC_TYPE_LONG;

    /**
     * The constant OMNI_VEC_TYPE_DOUBLE.
     */
    public static VecType OMNI_VEC_TYPE_DOUBLE;

    /**
     * The constant OMNI_VEC_TYPE_BOOLEAN.
     */
    public static VecType OMNI_VEC_TYPE_BOOLEAN;

    /**
     * The constant OMNI_VEC_TYPE_SHORT.
     */
    public static VecType OMNI_VEC_TYPE_SHORT;

    /**
     * The constant OMNI_VEC_TYPE_128_DECIMAL.
     */
    public static VecType OMNI_VEC_TYPE_128_DECIMAL;

    /**
     * The constant OMNI_VEC_TYPE_256_DECIMAL.
     */
    public static VecType OMNI_VEC_TYPE_256_DECIMAL;

    /**
     * The constant OMNI_VEC_TYPE_VARCHAR.
     */
    public static VecType OMNI_VEC_TYPE_VARCHAR;

    /**
     * The constant OMNI_VEC_TYPE_CONTAINER.
     */
    public static VecType OMNI_VEC_TYPE_CONTAINER;

    /**
     * The constant OMNI_VEC_TYPE_DICTIONARY.
     */
    public static VecType OMNI_VEC_TYPE_DICTIONARY;

    /**
     * Instantiates a new Vec type.
     *
     * @param value the value
     */
    public VecType(int value) {
        super(value);
    }
}
