/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type frame bound type for window operator.
 *
 * @since 2022-04-15
 */
public class OmniWindowFrameBoundType extends Constant {
    /**
     * The constant OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING.
     */
    public static OmniWindowFrameBoundType OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING;

    /**
     * The constant OMNI_FRAME_BOUND_PRECEDING.
     */
    public static OmniWindowFrameBoundType OMNI_FRAME_BOUND_PRECEDING;

    /**
     * The constant OMNI_FRAME_BOUND_CURRENT_ROW.
     */
    public static OmniWindowFrameBoundType OMNI_FRAME_BOUND_CURRENT_ROW;

    /**
     * The constant OMNI_FRAME_BOUND_FOLLOWING.
     */
    public static OmniWindowFrameBoundType OMNI_FRAME_BOUND_FOLLOWING;

    /**
     * The constant OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING.
     */
    public static OmniWindowFrameBoundType OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING;

    private static final long serialVersionUID = 3646147886114670835L;

    /**
     * Instantiates a new frame bound type.
     *
     * @param value the value
     */
    @JsonCreator
    public OmniWindowFrameBoundType(@JsonProperty("value") int value) {
        super(value);
    }
}
