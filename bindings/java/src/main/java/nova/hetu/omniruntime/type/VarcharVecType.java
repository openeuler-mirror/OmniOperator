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
public class VarcharVecType extends VecType {
    /**
     * Varchar singleton
     */
    public static final VarcharVecType VARCHAR = new VarcharVecType(Integer.MAX_VALUE);

    @JsonProperty
    private final int width;

    /**
     * The construct of varchar vector type
     *
     * @param width the width of varchar
     */
    public VarcharVecType(@JsonProperty("width") int width) {
        super(VecTypeId.OMNI_VEC_TYPE_VARCHAR);
        this.width = width;
    }

    public int getWidth() {
        return width;
    }
}
