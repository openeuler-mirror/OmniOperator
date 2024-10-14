/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * timestamp data type.
 *
 * @since 2024-09-09
 */
public class TimestampDataType extends DataType {
    /**
     * timestamp singleton.
     */
    public static final TimestampDataType TIMESTAMP = new TimestampDataType();

    private static final long serialVersionUID = -165184964293631557L;

    /**
     * The construct.
     */
    public TimestampDataType() {
        super(DataTypeId.OMNI_TIMESTAMP);
    }
}
