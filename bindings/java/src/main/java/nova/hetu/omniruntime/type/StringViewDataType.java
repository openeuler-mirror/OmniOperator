/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * string view data type.
 *
 * @since 2026-05-26
 */
public class StringViewDataType extends DataType {
    /**
     * StringView singleton.
     */
    public static final StringViewDataType STRING_VIEW = new StringViewDataType();

    private static final long serialVersionUID = 1794724451383359034L;

    public StringViewDataType() {
        super(DataTypeId.OMNI_STRING_VIEW);
    }
}
