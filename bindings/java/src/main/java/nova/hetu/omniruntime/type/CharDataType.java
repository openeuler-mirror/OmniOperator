/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * char data type.
 *
 * @since 2021-11-30
 */
public class CharDataType extends VarcharDataType {
    /**
     * max width for char data type.
     */
    public static final int MAX_WIDTH = 65_536;

    /**
     * char singleton.
     */
    public static final CharDataType CHAR = new CharDataType(MAX_WIDTH);

    private static final long serialVersionUID = -8306919387371983633L;

    /**
     * The construct of char data type.
     *
     * @param width the width of char
     */
    public CharDataType(@JsonProperty("width") int width) {
        super(width, DataTypeId.OMNI_CHAR);
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
        CharDataType other = (CharDataType) obj;
        return Objects.equals(width, other.getWidth()) && Objects.equals(super.getId(), other.getId());
    }
}
