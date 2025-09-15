/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * container data type.
 *
 * @since 2021-07-17
 */
public class ContainerDataType extends DataType {
    /**
     * Container singleton.
     */
    public static final ContainerDataType CONTAINER = new ContainerDataType();

    private static final long serialVersionUID = 7653293048781110462L;

    private DataType[] fieldTypes;

    /**
     * The construct of container data type.
     *
     * @param fieldTypes the types of data
     */
    public ContainerDataType(DataType[] fieldTypes) {
        super(DataTypeId.OMNI_CONTAINER);
        this.fieldTypes = fieldTypes;
    }

    /**
     * Container construct.
     */
    public ContainerDataType() {
        super(DataTypeId.OMNI_CONTAINER);
    }

    /**
     * get number of filed types.
     *
     * @return the number of filedTypes
     */
    public int size() {
        return fieldTypes.length;
    }

    /**
     * get field types.
     *
     * @return field types
     */
    public DataType[] getFieldTypes() {
        return fieldTypes;
    }
}
