/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import java.io.Serializable;
import java.util.Objects;

/**
 * The type Constant. The abstract class of all enum constant class
 *
 * @since 2021-06-30
 */
public abstract class Constant implements Serializable {
    private static final long serialVersionUID = -2589766491699675794L;

    private final int value;

    /**
     * Instantiates a new Constant.
     *
     * @param value the value
     */
    public Constant(int value) {
        this.value = value;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return ((Constant) obj).getValue() == value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
