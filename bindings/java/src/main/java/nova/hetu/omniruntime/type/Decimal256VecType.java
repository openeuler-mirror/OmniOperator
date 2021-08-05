/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class Decimal256VecType extends VecType {
    public static final int DEFAULT_PRECISION = 76;

    public static final int DEFAULT_SCALE = 0;

    /**
     * Decimal256 singleton
     */
    public static final Decimal256VecType DECIMAL256 = new Decimal256VecType(DEFAULT_PRECISION, DEFAULT_SCALE);

    @JsonProperty
    private final int precision;

    @JsonProperty
    private final int scale;

    /**
     * Construct of decimal256 vector type
     *
     * @param precision the precision of decimal
     * @param scale     the scale of decimal
     */
    public Decimal256VecType(@JsonProperty("precision") int precision, @JsonProperty("scale") int scale) {
        super(VecTypeId.OMNI_VEC_TYPE_DECIMAL256);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
