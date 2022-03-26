/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * short data type.
 *
 * @since 2021-08-05
 */
public class ShortDataType extends DataType {
    /**
     * Short singleton.
     */
    public static final ShortDataType SHORT = new ShortDataType();

    private static final long serialVersionUID = -1938040225939461L;

    /**
     * The construct.
     */
    public ShortDataType() {
        super(DataTypeId.OMNI_SHORT);
    }
}
