/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class IntVecType extends VecType {
    /**
     * Integer singleton
     */
    public static final IntVecType INTEGER = new IntVecType();

    /**
     * The construct
     */
    public IntVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_INT);
    }
}
