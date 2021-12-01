/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * char vec type
 *
 * @since 2021-11-30
 */
public class CharVecType extends VarcharVecType {
    /**
     * max width for char vec type
     */
    public static final int MAX_WIDTH = 65_536;

    /**
     * char singleton
     */
    public static final CharVecType CHAR = new CharVecType(MAX_WIDTH);

    /**
     * The construct of char vector type
     *
     * @param width the width of char
     */
    public CharVecType(@JsonProperty("width") int width) {
        super(width, VecTypeId.OMNI_VEC_TYPE_CHAR);
    }

    @Override
    public int hashCode() {
        return Objects.hash(width, super.getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CharVecType other = (CharVecType) obj;
        return Objects.equals(width, other.getWidth()) && Objects.equals(super.getId(), other.getId());
    }
}
