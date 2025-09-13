/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * long data type.
 *
 * @since 2021-08-05
 */
public class LongDataType extends DataType {
    /**
     * Long singleton.
     */
    public static final LongDataType LONG = new LongDataType();

    private static final long serialVersionUID = -1589352305079680921L;

    /**
     * The construct.
     */
    public LongDataType() {
        super(DataTypeId.OMNI_LONG);
    }
}
