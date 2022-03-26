/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * int data type.
 *
 * @since 2021-08-05
 */
public class IntDataType extends DataType {
    /**
     * Integer singleton.
     */
    public static final IntDataType INTEGER = new IntDataType();

    private static final long serialVersionUID = -5622723228847479686L;

    /**
     * The construct.
     */
    public IntDataType() {
        super(DataTypeId.OMNI_INT);
    }
}
