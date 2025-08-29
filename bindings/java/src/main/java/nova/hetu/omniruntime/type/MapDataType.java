/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * MAP data type.
 *
 * @since 2025-8-29
 */
public class MapDataType extends DataType {
    /**
     * MAP singleton.
     */
    public static final MapDataType MAP = new MapDataType();

    private static final long serialVersionUID = -5068261859604278302L;

    private DataType keyType;

    private DataType valueType;

    /**
     * The construct of MAP data type.
     *
     * @param keyType the type of key data
     * @param valueType the type of value data
     */
    public MapDataType(DataType keyType, DataType valueType) {
        super(DataTypeId.OMNI_MAP);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /**
     * Container construct.
     */
    public MapDataType() {
        super(DataTypeId.OMNI_MAP);
    }


    public int size() {
        return 2;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }
}
