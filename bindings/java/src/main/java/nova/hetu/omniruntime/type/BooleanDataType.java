/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * boolean data type.
 *
 * @since 2021-08-05
 */
public class BooleanDataType extends DataType {
    /**
     * Boolean singleton.
     */
    public static final BooleanDataType BOOLEAN = new BooleanDataType();

    private static final long serialVersionUID = 8981310620537140335L;

    /**
     * Boolean construct.
     */
    public BooleanDataType() {
        super(DataTypeId.OMNI_BOOLEAN);
    }
}
