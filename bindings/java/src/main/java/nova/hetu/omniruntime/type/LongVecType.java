/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class LongVecType extends VecType {
    /**
     * Long singleton
     */
    public static final LongVecType LONG = new LongVecType();

    /**
     * The construct
     */
    public LongVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_LONG);
    }
}
