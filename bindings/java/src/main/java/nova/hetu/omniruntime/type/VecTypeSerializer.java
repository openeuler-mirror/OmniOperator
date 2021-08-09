/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;
import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_NOSUPPORT;

/**
 * Vector type serializer, used for serialize and deserialize the vector type.
 *
 * @since 2021-08-05
 */
public class VecTypeSerializer {
    /**
     * Object mapper singleton
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Serialize a single vector type.
     *
     * @param vecType serialize a single vector type.
     * @return return the string after serialization.
     */
    public static String serializeSingle(VecType vecType) {
        VecTypeExt vecTypeExt = toVecTypeExt(vecType);

        try {
            return OBJECT_MAPPER.writeValueAsString(vecTypeExt);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Serialization failed.", e);
        }
    }

    /**
     * Serialize vector types.
     *
     * @param vecTypes serialize vector types.
     * @return return the string after serialization.
     */
    public static String serialize(VecType[] vecTypes) {
        VecTypeExt[] vecTypeExts = new VecTypeExt[vecTypes.length];
        for (int i = 0; i < vecTypes.length; i++) {
            vecTypeExts[i] = toVecTypeExt(vecTypes[i]);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(vecTypeExts);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Serialization failed.", e);
        }
    }

    /**
     * Deserialize a single vector type.
     *
     * @param type the string need to be deserialization.
     * @return return the vector type
     */
    public static VecType deserializeSingle(String type) {
        try {
            VecTypeExt vecTypeExt = OBJECT_MAPPER.readerFor(VecTypeExt.class).readValue(type);
            return fromVecTypeExt(vecTypeExt);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Deserialization failed.", e);
        }
    }

    /**
     * Deserialize vector types.
     *
     * @param types the string need to be deserialization.
     * @return return the vector types.
     */
    public static VecType[] deserialize(String types) {
        try {
            VecTypeExt[] vecTypeExts = OBJECT_MAPPER.readerFor(VecTypeExt[].class).readValue(types);
            VecType[] vecTypes = new VecType[vecTypeExts.length];
            for (int i = 0; i < vecTypeExts.length; i++) {
                vecTypes[i] = fromVecTypeExt(vecTypeExts[i]);
            }
            return vecTypes;
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Deserialization failed.", e);
        }
    }

    private static VecTypeExt toVecTypeExt(VecType type) {
        VecTypeExt typeExt = new VecTypeExt(type.getId());
        switch (type.getId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DOUBLE:
            case OMNI_VEC_TYPE_BOOLEAN:
            case OMNI_VEC_TYPE_SHORT:
            case OMNI_VEC_TYPE_CONTAINER:
            case OMNI_VEC_TYPE_DICTIONARY:
            case OMNI_VEC_TYPE_INVALID:
                return typeExt;
            case OMNI_VEC_TYPE_VARCHAR:
                if (type instanceof VarcharVecType) {
                    return typeExt.setWidth(((VarcharVecType) type).getWidth());
                }
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                if (type instanceof Decimal128VecType) {
                    return typeExt.setPrecision(((Decimal128VecType) type).getPrecision())
                            .setScale(((Decimal128VecType) type).getScale());
                }
                break;
            case OMNI_VEC_TYPE_DECIMAL256:
                if (type instanceof Decimal256VecType) {
                    return typeExt.setPrecision(((Decimal256VecType) type).getPrecision())
                            .setScale(((Decimal256VecType) type).getScale());
                }
                break;
            case OMNI_VEC_TYPE_DATE32:
                if (type instanceof Date32VecType) {
                    return typeExt.setDateUnit(((Date32VecType) type).getDateUnit());
                }
                break;
            case OMNI_VEC_TYPE_DATE64:
                if (type instanceof Date64VecType) {
                    return typeExt.setDateUnit(((Date64VecType) type).getDateUnit());
                }
                break;
            default:
                throw new OmniRuntimeException(OMNI_NOSUPPORT, "Unsupported vector type : " + type.getId());
        }
        throw new OmniRuntimeException(OMNI_NOSUPPORT, "Unsupported vector type : " + type.getId());
    }

    private static VecType fromVecTypeExt(VecTypeExt typeExt) {
        switch (typeExt.getId()) {
            case OMNI_VEC_TYPE_INT:
                return IntVecType.INTEGER;
            case OMNI_VEC_TYPE_LONG:
                return LongVecType.LONG;
            case OMNI_VEC_TYPE_DOUBLE:
                return DoubleVecType.DOUBLE;
            case OMNI_VEC_TYPE_BOOLEAN:
                return BooleanVecType.BOOLEAN;
            case OMNI_VEC_TYPE_SHORT:
                return ShortVecType.SHORT;
            case OMNI_VEC_TYPE_CONTAINER:
                return ContainerVecType.CONTAINER;
            case OMNI_VEC_TYPE_DICTIONARY:
                return DictionaryVecType.DICTIONARY;
            case OMNI_VEC_TYPE_NONE:
                return VecType.NONE;
            case OMNI_VEC_TYPE_INVALID:
                return VecType.INVALID;
            case OMNI_VEC_TYPE_VARCHAR:
                return new VarcharVecType(typeExt.getWidth());
            case OMNI_VEC_TYPE_DECIMAL128:
                return new Decimal128VecType(typeExt.getPrecision(), typeExt.getScale());
            case OMNI_VEC_TYPE_DECIMAL256:
                return new Decimal256VecType(typeExt.getPrecision(), typeExt.getScale());
            case OMNI_VEC_TYPE_DATE32:
                return new Date32VecType(typeExt.getDateUnit());
            case OMNI_VEC_TYPE_DATE64:
                return new Date64VecType(typeExt.getDateUnit());
            default:
                throw new OmniRuntimeException(OMNI_NOSUPPORT, "Unsupported vector type : " + typeExt.getId());
        }
    }

    private static class VecTypeExt {
        @JsonProperty
        private final VecType.VecTypeId id;

        @JsonProperty
        private int width;

        @JsonProperty
        private int precision;

        @JsonProperty
        private int scale;

        @JsonProperty
        private VecType.DateUnit dateUnit;

        @JsonProperty
        private VecType.TimeUnit timeUnit;

        public VecTypeExt(VecType.VecTypeId id) {
            this(id, 0, 0, 0, VecType.DateUnit.DAY, VecType.TimeUnit.SEC);
        }

        @JsonCreator
        public VecTypeExt(@JsonProperty("id") VecType.VecTypeId id, @JsonProperty("width") int width,
                @JsonProperty("precision") int precision, @JsonProperty("scale") int scale,
                @JsonProperty("dateUnit") VecType.DateUnit dateUnit,
                @JsonProperty("timeUnit") VecType.TimeUnit timeUnit) {
            this.id = id;
            this.width = width;
            this.precision = precision;
            this.scale = scale;
            this.dateUnit = dateUnit;
            this.timeUnit = timeUnit;
        }

        public VecType.VecTypeId getId() {
            return id;
        }

        public int getWidth() {
            return width;
        }

        VecTypeExt setWidth(int width) {
            this.width = width;
            return this;
        }

        public int getPrecision() {
            return precision;
        }

        VecTypeExt setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public int getScale() {
            return scale;
        }

        VecTypeExt setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public VecType.DateUnit getDateUnit() {
            return dateUnit;
        }

        VecTypeExt setDateUnit(VecType.DateUnit dateUnit) {
            this.dateUnit = dateUnit;
            return this;
        }

        public VecType.TimeUnit getTimeUnit() {
            return timeUnit;
        }

        VecTypeExt setTimeUnit(VecType.TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }
    }
}
