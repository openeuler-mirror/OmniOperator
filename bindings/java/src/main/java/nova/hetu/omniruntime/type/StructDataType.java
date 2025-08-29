/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * STRUCT data type.
 *
 * @since 2025-8-29
 */
public class StructDataType extends DataType {
    /**
     * STRUCT singleton.
     */
    public static final StructDataType STRUCT = new StructDataType();

    private static final long serialVersionUID = -6904326102609779407L;

    private DataType[] fieldTypes;

    /**
     * The construct of STRUCT data type.
     *
     * @param fieldTypes the types of data
     */
    public StructDataType(DataType[] fieldTypes) {
        super(DataTypeId.OMNI_ROW);
        this.fieldTypes = fieldTypes;
    }

    /**
     * Container construct.
     */
    public StructDataType() {
        super(DataTypeId.OMNI_ROW);
    }

    /**
     * get number of field types.
     *
     * @return the number of fieldTypes
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
