/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Status.
 *
 * @since 20210630
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
