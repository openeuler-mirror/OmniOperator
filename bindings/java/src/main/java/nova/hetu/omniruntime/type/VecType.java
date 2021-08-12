/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class VecType {
    /**
     * It is a none vector type
     */
    public static final VecType NONE = new VecType(VecTypeId.OMNI_VEC_TYPE_NONE);

    /**
     * It is a invalid vector type
     */
    public static final VecType INVALID = new VecType(VecTypeId.OMNI_VEC_TYPE_INVALID);

    @JsonProperty
    private final VecTypeId id;

    public VecType(@JsonProperty("id") VecTypeId id) {
        this.id = id;
    }

    public VecTypeId getId() {
        return id;
    }

    /**
     * The vector type id
     */
    public enum VecTypeId {
        OMNI_VEC_TYPE_NONE,
        OMNI_VEC_TYPE_INT,
        OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_DOUBLE,
        OMNI_VEC_TYPE_BOOLEAN,
        OMNI_VEC_TYPE_SHORT,
        OMNI_VEC_TYPE_DECIMAL64,
        OMNI_VEC_TYPE_DECIMAL128,
        OMNI_VEC_TYPE_DATE32,
        OMNI_VEC_TYPE_DATE64,
        OMNI_VEC_TYPE_TIME32,
        OMNI_VEC_TYPE_TIME64,
        OMNI_VEC_TYPE_TIMESTAMP,
        OMNI_VEC_TYPE_INTERVAL_MONTHS,
        OMNI_VEC_TYPE_INTERVAL_DAY_TIME,
        OMNI_VEC_TYPE_VARCHAR,
        OMNI_VEC_TYPE_DICTIONARY,
        OMNI_VEC_TYPE_CONTAINER,
        OMNI_VEC_TYPE_INVALID
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
}
