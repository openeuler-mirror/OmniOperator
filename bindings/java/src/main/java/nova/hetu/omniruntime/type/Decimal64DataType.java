/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Decimal64 data type
 *
 * @since 2021-08-05
 */
public class Decimal64DataType extends DataType {
    /**
     * Default precision value of decimal.
     */
    public static final int DEFAULT_PRECISION = 19;

    /**
     * Default scale value of decimal.
     */
    public static final int DEFAULT_SCALE = 0;

    /**
     * Decimal64 singleton
     */
    public static final Decimal64DataType DECIMAL64 = new Decimal64DataType(DEFAULT_PRECISION, DEFAULT_SCALE);

    @JsonProperty
    private final int precision;

    @JsonProperty
    private final int scale;

    /**
     * Construct of decimal64 vector type
     *
     * @param precision the precision of decimal
     * @param scale the scale of decimal
     */
    public Decimal64DataType(@JsonProperty("precision") int precision, @JsonProperty("scale") int scale) {
        super(DataTypeId.OMNI_DATA_TYPE_DECIMAL64);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(precision, scale, super.getId());
    }
}
