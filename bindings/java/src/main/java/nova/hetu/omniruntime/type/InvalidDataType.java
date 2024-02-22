/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * invalid data type. The data type of unsupported/invalid data.
 *
 * @since 2022-04-01
 */
public class InvalidDataType extends DataType {
    /**
     * Invalid singleton.
     */
    public static final InvalidDataType INVALID = new InvalidDataType();

    /**
     * The construct.
     */
    public InvalidDataType() {
        super(DataTypeId.OMNI_INVALID);
    }
}
