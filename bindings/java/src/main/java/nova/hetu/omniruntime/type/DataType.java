/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * base class of data type
 *
 * @since 2021-08-05
 */
public class DataType implements Serializable {
    /**
     * It is a none data type
     */
    public static final DataType NONE = new DataType(DataTypeId.OMNI_DATA_TYPE_NONE);

    /**
     * It is a invalid data type
     */
    public static final DataType INVALID = new DataType(DataTypeId.OMNI_DATA_TYPE_INVALID);

    private static final long serialVersionUID = 2589766491688675794L;

    @JsonProperty
    private final DataTypeId id;

    public DataType(@JsonProperty("id") DataTypeId id) {
        this.id = id;
    }

    public DataTypeId getId() {
        return id;
    }

    /**
     * Create a vector type object.
     *
     * @param typeId create vector type by vector type id.
     * @return vector type.
     */
    public static DataType create(int typeId) {
        return new DataType(DataTypeId.values()[typeId]);
    }

    /**
     * The vector type id
     */
    public enum DataTypeId {
        OMNI_DATA_TYPE_NONE,
        OMNI_DATA_TYPE_INT,
        OMNI_DATA_TYPE_LONG,
        OMNI_DATA_TYPE_DOUBLE,
        OMNI_DATA_TYPE_BOOLEAN,
        OMNI_DATA_TYPE_SHORT,
        OMNI_DATA_TYPE_DECIMAL64,
        OMNI_DATA_TYPE_DECIMAL128,
        OMNI_DATA_TYPE_DATE32,
        OMNI_DATA_TYPE_DATE64,
        OMNI_DATA_TYPE_TIME32,
        OMNI_DATA_TYPE_TIME64,
        OMNI_DATA_TYPE_TIMESTAMP,
        OMNI_DATA_TYPE_INTERVAL_MONTHS,
        OMNI_DATA_TYPE_INTERVAL_DAY_TIME,
        OMNI_DATA_TYPE_VARCHAR,
        OMNI_DATA_TYPE_CHAR,
        OMNI_DATA_TYPE_CONTAINER,
        OMNI_DATA_TYPE_INVALID
    }

    /**
     * The unit of date
     */
    public enum DateUnit {
        DAY,
        MILLI
    }

    /**
     * The unit of time
     */
    public enum TimeUnit {
        SEC,
        MILLISEC,
        MICROSEC,
        NANOSEC
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType dataType = (DataType) o;
        return id == dataType.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
