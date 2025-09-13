/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import java.io.Serializable;
import java.util.Objects;

/**
 * data type.
 *
 * @since 2021-08-05
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "id")
@JsonTypeIdResolver(DataTypeSerializer.DataTypeResolver.class)
public class DataType implements Serializable {
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
        OMNI_NONE(0),
        OMNI_INT(1),
        OMNI_LONG(2),
        OMNI_DOUBLE(3),
        OMNI_BOOLEAN(4),
        OMNI_SHORT(5),
        OMNI_DECIMAL64(6),
        OMNI_DECIMAL128(7),
        OMNI_DATE32(8),
        OMNI_DATE64(9),
        OMNI_TIME32(10),
        OMNI_TIME64(11),
        OMNI_TIMESTAMP(12),
        OMNI_INTERVAL_MONTHS(13),
        OMNI_INTERVAL_DAY_TIME(14),
        OMNI_VARCHAR(15),
        OMNI_CHAR(16),
        OMNI_CONTAINER(17),
        OMNI_INVALID(18);

        private final int value;

        DataTypeId(int value) {
            this.value = value;
        }

        /**
         * Serialize the ordinal of enum.
         *
         * @return the ordinal.
         */
        @JsonValue
        public int toValue() {
            return this.value;
        }
    }

    /**
     * The unit of date.
     */
    public enum DateUnit {
        DAY(0),
        MILLI(1);

        private final int value;

        DateUnit(int value) {
            this.value = value;
        }

        /**
         * Serialize the value of enum.
         *
         * @return the value.
         */
        @JsonValue
        public int toValue() {
            return this.value;
        }
    }

    /**
     * The unit of time.
     */
    public enum TimeUnit {
        SEC(0),
        MILLISEC(1),
        MICROSEC(2),
        NANOSEC(3);

        private final int value;

        TimeUnit(int value) {
            this.value = value;
        }

        /**
         * Serialize the value of enum.
         *
         * @return the value.
         */
        @JsonValue
        public int toValue() {
            return this.value;
        }
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
