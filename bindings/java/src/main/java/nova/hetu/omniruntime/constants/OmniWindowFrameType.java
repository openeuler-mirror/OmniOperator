/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type window frame type.
 *
 * @since 2022-04-15
 */
public class OmniWindowFrameType extends Constant {
    /**
     * The constant OMNI_FRAME_TYPE_RANGE.
     */
    public static OmniWindowFrameType OMNI_FRAME_TYPE_RANGE;

    /**
     * The constant OMNI_FRAME_TYPE_ROWS.
     */
    public static OmniWindowFrameType OMNI_FRAME_TYPE_ROWS;

    private static final long serialVersionUID = -3453499979001168717L;

    /**
     * Instantiates a new window frame type.
     *
     * @param value the value
     */
    @JsonCreator
    public OmniWindowFrameType(@JsonProperty("value") int value) {
        super(value);
    }
}
