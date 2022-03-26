/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;
import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_NOSUPPORT;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nova.hetu.omniruntime.utils.OmniRuntimeException;

/**
 * Data type serializer, used for serialize and deserialize the data type.
 *
 * @since 2021-08-05
 */
public class DataTypeSerializer {
    /**
     * Object mapper singleton.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Serialize a single data type.
     *
     * @param dataType serialize a single data type
     * @return return the string after serialization
     */
    public static String serializeSingle(DataType dataType) {
        DataTypeExt dataTypeExt = toDataTypeExt(dataType);

        try {
            return OBJECT_MAPPER.writeValueAsString(dataTypeExt);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Serialization failed.", e);
        }
    }

    /**
     * Serialize data types.
     *
     * @param dataTypes serialize data types
     * @return return the string after serialization
     */
    public static String serialize(DataType[] dataTypes) {
        DataTypeExt[] dataTypeExts = new DataTypeExt[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypeExts[i] = toDataTypeExt(dataTypes[i]);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(dataTypeExts);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Serialization failed.", e);
        }
    }

    /**
     * Deserialize a single data type.
     *
     * @param type the string need to be deserialization
     * @return return the vector type
     */
    public static DataType deserializeSingle(String type) {
        try {
            DataTypeExt dataTypeExt = OBJECT_MAPPER.readerFor(DataTypeExt.class).readValue(type);
            return fromDataTypeExt(dataTypeExt);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Deserialization failed.", e);
        }
    }

    /**
     * Deserialize data types.
     *
     * @param types the string need to be deserialization
     * @return return the vector types
     */
    public static DataType[] deserialize(String types) {
        try {
            DataTypeExt[] dataTypeExts = OBJECT_MAPPER.readerFor(DataTypeExt[].class).readValue(types);
            DataType[] dataTypes = new DataType[dataTypeExts.length];
            for (int i = 0; i < dataTypeExts.length; i++) {
                dataTypes[i] = fromDataTypeExt(dataTypeExts[i]);
            }
            return dataTypes;
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Deserialization failed.", e);
        }
    }

    private static DataTypeExt toDataTypeExt(DataType type) {
        DataTypeExt typeExt = new DataTypeExt(type.getId());
        switch (type.getId()) {
            case OMNI_INT:
            case OMNI_LONG:
            case OMNI_DOUBLE:
            case OMNI_BOOLEAN:
            case OMNI_SHORT:
            case OMNI_CONTAINER:
            case OMNI_INVALID:
                return typeExt;
            case OMNI_VARCHAR:
                if (type instanceof VarcharDataType) {
                    typeExt.setWidth(((VarcharDataType) type).getWidth());
                }
                break;
            case OMNI_CHAR:
                if (type instanceof CharDataType) {
                    typeExt.setWidth(((CharDataType) type).getWidth());
                }
                break;
            case OMNI_DECIMAL64:
                if (type instanceof Decimal64DataType) {
                    typeExt.setPrecision(((Decimal64DataType) type).getPrecision())
                            .setScale(((Decimal64DataType) type).getScale());
                }
                break;
            case OMNI_DECIMAL128:
                if (type instanceof Decimal128DataType) {
                    typeExt.setPrecision(((Decimal128DataType) type).getPrecision())
                            .setScale(((Decimal128DataType) type).getScale());
                }
                break;
            case OMNI_DATE32:
                if (type instanceof Date32DataType) {
                    typeExt.setDateUnit(((Date32DataType) type).getDateUnit());
                }
                break;
            case OMNI_DATE64:
                if (type instanceof Date64DataType) {
                    typeExt.setDateUnit(((Date64DataType) type).getDateUnit());
                }
                break;
            default:
                throw new OmniRuntimeException(OMNI_NOSUPPORT, "Unsupported data type : " + type.getId());
        }
        return typeExt;
    }

    private static DataType fromDataTypeExt(DataTypeExt typeExt) {
        switch (typeExt.getId()) {
            case OMNI_INT:
                return IntDataType.INTEGER;
            case OMNI_LONG:
                return LongDataType.LONG;
            case OMNI_DOUBLE:
                return DoubleDataType.DOUBLE;
            case OMNI_BOOLEAN:
                return BooleanDataType.BOOLEAN;
            case OMNI_SHORT:
                return ShortDataType.SHORT;
            case OMNI_CONTAINER:
                return ContainerDataType.CONTAINER;
            case OMNI_NONE:
                return DataType.NONE;
            case OMNI_INVALID:
                return DataType.INVALID;
            case OMNI_VARCHAR:
                return new VarcharDataType(typeExt.getWidth());
            case OMNI_CHAR:
                return new CharDataType(typeExt.getWidth());
            case OMNI_DECIMAL64:
                return new Decimal64DataType(typeExt.getPrecision(), typeExt.getScale());
            case OMNI_DECIMAL128:
                return new Decimal128DataType(typeExt.getPrecision(), typeExt.getScale());
            case OMNI_DATE32:
                return new Date32DataType(typeExt.getDateUnit());
            case OMNI_DATE64:
                return new Date64DataType(typeExt.getDateUnit());
            default:
                throw new OmniRuntimeException(OMNI_NOSUPPORT, "Unsupported data type : " + typeExt.getId());
        }
    }

    private static class DataTypeExt {
        @JsonProperty
        private final DataType.DataTypeId id;

        @JsonProperty
        private int width;

        @JsonProperty
        private int precision;

        @JsonProperty
        private int scale;

        @JsonProperty
        private DataType.DateUnit dateUnit;

        @JsonProperty
        private DataType.TimeUnit timeUnit;

        public DataTypeExt(DataType.DataTypeId id) {
            this(id, 0, 0, 0, DataType.DateUnit.DAY, DataType.TimeUnit.SEC);
        }

        @JsonCreator
        public DataTypeExt(@JsonProperty("id") DataType.DataTypeId id, @JsonProperty("width") int width,
                @JsonProperty("precision") int precision, @JsonProperty("scale") int scale,
                @JsonProperty("dateUnit") DataType.DateUnit dateUnit,
                @JsonProperty("timeUnit") DataType.TimeUnit timeUnit) {
            this.id = id;
            this.width = width;
            this.precision = precision;
            this.scale = scale;
            this.dateUnit = dateUnit;
            this.timeUnit = timeUnit;
        }

        public DataType.DataTypeId getId() {
            return id;
        }

        public int getWidth() {
            return width;
        }

        DataTypeExt setWidth(int width) {
            this.width = width;
            return this;
        }

        public int getPrecision() {
            return precision;
        }

        DataTypeExt setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public int getScale() {
            return scale;
        }

        DataTypeExt setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public DataType.DateUnit getDateUnit() {
            return dateUnit;
        }

        DataTypeExt setDateUnit(DataType.DateUnit dateUnit) {
            this.dateUnit = dateUnit;
            return this;
        }

        public DataType.TimeUnit getTimeUnit() {
            return timeUnit;
        }

        DataTypeExt setTimeUnit(DataType.TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }
    }
}
