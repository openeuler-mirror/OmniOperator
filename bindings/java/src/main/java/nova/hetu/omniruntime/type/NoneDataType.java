/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * None data type. The data type of NULL data.
 *
 * @since 2022-04-01
 */
public class NoneDataType extends DataType {
    /**
     * None singleton.
     */
    public static final NoneDataType NONE = new NoneDataType();

    /**
     * The construct.
     */
    public NoneDataType() {
        super(DataTypeId.OMNI_NONE);
    }
}
