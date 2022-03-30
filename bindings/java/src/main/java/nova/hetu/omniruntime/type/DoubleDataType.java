/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * double data type.
 *
 * @since 2021-08-05
 */
public class DoubleDataType extends DataType {
    /**
     * Double singleton.
     */
    public static final DoubleDataType DOUBLE = new DoubleDataType();

    private static final long serialVersionUID = -5517157056853810138L;

    /**
     * The construct.
     */
    public DoubleDataType() {
        super(DataTypeId.OMNI_DOUBLE);
    }
}
