/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * data type.
 *
 * @since 2021-08-05
 */
public class DataType implements Serializable {
    /**
     * It is a none data type.
     */
    public static final DataType NONE = new DataType(DataTypeId.OMNI_NONE);

    /**
     * It is a invalid data type.
     */
    public static final DataType INVALID = new DataType(DataTypeId.OMNI_INVALID);

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
     * Create a data type object.
     *
     * @param typeId create data type by data type id
     * @return data type
     */
    public static DataType create(int typeId) {
        return new DataType(DataTypeId.values()[typeId]);
    }

    /**
     * The data type id.
     */
    public enum DataTypeId {
        OMNI_NONE,
        OMNI_INT,
        OMNI_LONG,
        OMNI_DOUBLE,
        OMNI_BOOLEAN,
        OMNI_SHORT,
        OMNI_DECIMAL64,
        OMNI_DECIMAL128,
        OMNI_DATE32,
        OMNI_DATE64,
        OMNI_TIME32,
        OMNI_TIME64,
        OMNI_TIMESTAMP,
        OMNI_INTERVAL_MONTHS,
        OMNI_INTERVAL_DAY_TIME,
        OMNI_VARCHAR,
        OMNI_CHAR,
        OMNI_CONTAINER,
        OMNI_INVALID
    }

    /**
     * The unit of date.
     */
    public enum DateUnit {
        DAY,
        MILLI
    }

    /**
     * The unit of time.
     */
    public enum TimeUnit {
        SEC,
        MILLISEC,
        MICROSEC,
        NANOSEC
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DataType dataType = (DataType) obj;
        return id == dataType.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
