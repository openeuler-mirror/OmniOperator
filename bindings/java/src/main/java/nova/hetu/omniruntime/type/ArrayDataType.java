/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * array data type.
 *
 * @since 2025-08-28
 */
public class ArrayDataType extends DataType {
    /**
     * Array singleton.
     */
    public static final ArrayDataType ARRAY = new ArrayDataType();

    private static final long serialVersionUID = 8915367144067644693L;

    private DataType child;

    /**
     * The construct of array data type.
     *
     * @param child the type of elements in an array
     */
    public ArrayDataType(DataType child) {
        super(DataTypeId.OMNI_ARRAY);
        this.child = child;
    }

    /**
     * Array construct.
     */
    public ArrayDataType() {
        super(DataTypeId.OMNI_ARRAY);
    }

    /**
     * Get number of child.
     *
     * @return the number of child
     */
    public int size() {
        return 1;
    }

    /**
     * Get child.
     *
     * @return child
     */
    public DataType getElementType() {
        return child;
    }
}
