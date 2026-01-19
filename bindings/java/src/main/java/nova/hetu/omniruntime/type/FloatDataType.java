/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * float data type.
 *
 * @since 2021-08-05
 */
public class FloatDataType extends DataType {
    /**
     * Float singleton.
     */
    public static final FloatDataType FLOAT = new FloatDataType();

    /**
     * The construct.
     */
    public FloatDataType() {
        super(DataTypeId.OMNI_FLOAT);
    }
}
