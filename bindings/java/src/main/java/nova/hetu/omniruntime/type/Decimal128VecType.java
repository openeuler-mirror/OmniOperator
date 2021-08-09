/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class Decimal128VecType extends VecType {
    public static final int DEFAULT_PRECISION = 38;

    public static final int DEFAULT_SCALE = 0;

    /**
     * Decimal128 singleton
     */
    public static final Decimal128VecType DECIMAL128 = new Decimal128VecType(DEFAULT_PRECISION, DEFAULT_SCALE);

    @JsonProperty
    private final int precision;

    @JsonProperty
    private final int scale;

    /**
     * Construct of decima128 vector type.
     *
     * @param precision the precision of decimal.
     * @param scale     the scale of decimal.
     */
    @JsonCreator
    public Decimal128VecType(@JsonProperty("precision") int precision, @JsonProperty("scale") int scale) {
        super(VecTypeId.OMNI_VEC_TYPE_DECIMAL128);
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
