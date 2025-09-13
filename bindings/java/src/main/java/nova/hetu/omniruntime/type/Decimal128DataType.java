/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Decimal128 data type.
 *
 * @since 2021-08-05
 */
public class Decimal128DataType extends DecimalDataType {
    /**
     * Default precision value of decimal.
     */
    public static final int DEFAULT_PRECISION = 38;

    /**
     * Default scale value of decimal.
     */
    public static final int DEFAULT_SCALE = 0;

    /**
     * Decimal128 singleton.
     */
    public static final Decimal128DataType DECIMAL128 = new Decimal128DataType(DEFAULT_PRECISION, DEFAULT_SCALE);

    /**
     * Construct of decimal128 data type.
     *
     * @param precision the precision of decimal
     * @param scale the scale of decimal
     */

    private static final long serialVersionUID = 7504240180082236146L;

    @JsonCreator
    public Decimal128DataType(@JsonProperty("precision") int precision, @JsonProperty("scale") int scale) {
        super(precision, scale, DataTypeId.OMNI_DECIMAL128);
    }
}
