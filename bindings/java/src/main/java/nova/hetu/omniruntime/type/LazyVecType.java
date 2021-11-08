/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * date32 vec type
 *
 * @since 2021-11-01
 */
public class LazyVecType extends VecType {
    /**
     * Long singleton
     */
    public static final LazyVecType LAZY = new LazyVecType();

    /**
     * The construct
     */
    public LazyVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_LAZY);
    }
}
