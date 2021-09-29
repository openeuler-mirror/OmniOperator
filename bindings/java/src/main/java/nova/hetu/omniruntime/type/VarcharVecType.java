/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class VarcharVecType extends VecType {
    /**
     * max width for varchar vec type
     */
    public static final int MAX_WIDTH = 1024 * 1024;

    /**
     * Varchar singleton
     */
    public static final VarcharVecType VARCHAR = new VarcharVecType(MAX_WIDTH);

    @JsonProperty
    private final int width;

    /**
     * The construct of varchar vector type
     *
     * @param width the width of varchar
     */
    public VarcharVecType(@JsonProperty("width") int width) {
        super(VecTypeId.OMNI_VEC_TYPE_VARCHAR);
        if (width > MAX_WIDTH) {
            this.width = MAX_WIDTH;
        } else {
            this.width = width;
        }
    }

    public int getWidth() {
        return width;
    }

    @Override
    public int hashCode() {
        return Objects.hash(width, super.getId());
    }
}
