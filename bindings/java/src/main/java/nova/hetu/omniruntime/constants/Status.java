/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Status.
 *
 * @since 2021-06-30
 */
@SuppressWarnings("StaticVariableName")
public class Status extends Constant {
    /**
     * The constant OMNI_STATUS_NORMAL.
     */
    public static Status OMNI_STATUS_NORMAL;

    /**
     * The constant OMNI_STATUS_ERROR.
     */
    public static Status OMNI_STATUS_ERROR;

    /**
     * The constant OMNI_STATUS_FINISHED.
     */
    public static Status OMNI_STATUS_FINISHED;

    private static final long serialVersionUID = -3424552555224669902L;

    /**
     * Instantiates a new Status.
     *
     * @param value the value
     */
    @JsonCreator
    public Status(@JsonProperty("value") int value) {
        super(value);
    }
}
