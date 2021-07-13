/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import nova.hetu.omniruntime.OmniLibs;

import java.util.Objects;

/**
 * The type Constant.
 *
 * @since 20210630
 */
@SuppressWarnings("StaticVariableName")
public abstract class Constant {
    static {
        OmniLibs.load();
        loadConstants();
    }

    private final int value;

    /**
     * Instantiates a new Constant.
     *
     * @param value the value
     */
    public Constant(int value) {
        this.value = value;
    }

    private static native void loadConstants();

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
