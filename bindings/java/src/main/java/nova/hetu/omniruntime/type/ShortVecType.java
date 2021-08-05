/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class ShortVecType extends VecType {
    /**
     * Short singleton
     */
    public static final ShortVecType SHORT = new ShortVecType();

    /**
     * The construct
     */
    public ShortVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_SHORT);
    }
}
