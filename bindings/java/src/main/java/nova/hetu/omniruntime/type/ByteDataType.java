/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * byte data type.
 *
 * @since 2025-08-05
 */
public class ByteDataType extends DataType {
    /**
     * Byte singleton.
     */
    public static final ByteDataType BYTE = new ByteDataType();

    private static final long serialVersionUID = -1145142315179689320L;

    /**
     * The construct.
     */
    public ByteDataType() {
        super(DataTypeId.OMNI_BYTE);
    }
}
