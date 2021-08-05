/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * boolean vec type
 *
 * @since 2021-08-05
 */
public class BooleanVecType extends VecType {
    /**
     * Boolean singleton
     */
    public static final BooleanVecType BOOLEAN = new BooleanVecType();

    /**
     * Boolean construct
     */
    public BooleanVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_BOOLEAN);
    }
}
