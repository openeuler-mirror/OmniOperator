/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * container vec type
 *
 * @since 2021-07-17
 */
public class ContainerVecType extends VecType {
    /**
     * Container singleton
     */
    public static final ContainerVecType CONTAINER = new ContainerVecType();

    private VecType[] fieldTypes;

    public ContainerVecType(VecType[] fieldTypes) {
        super(VecTypeId.OMNI_VEC_TYPE_CONTAINER);
        this.fieldTypes = fieldTypes;
    }

    /**
     * Container construct
     */
    public ContainerVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_CONTAINER);
    }

    /**
     * get number of filed types
     *
     * @return the number of filedTypes
     */
    public int size() {
        return fieldTypes.length;
    }

    /**
     * get field types
     *
     * @return field types
     */
    public VecType[] getFieldTypes() {
        return fieldTypes;
    }
}
