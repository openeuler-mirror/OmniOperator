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
    // Optional field names for this struct. If not provided, downstream may
    // treat fields as ordinal-based.
    private String[] fieldNames;

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
     * The construct of STRUCT data type with field names.
     *
     * @param fieldTypes the types of fields
     * @param fieldNames the names of fields (same length as fieldTypes)
     */
    public StructDataType(DataType[] fieldTypes, String[] fieldNames) {
        super(DataTypeId.OMNI_ROW);
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
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

    /**
     * get field names.
     *
     * @return field names, may be null if not provided
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    public DataType getFieldType(int index) {
        return fieldTypes[index];
    }
}
