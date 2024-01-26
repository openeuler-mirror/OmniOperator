/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type frame bound type for window operator.
 *
 * @since 2022-04-15
 */
public class OmniWindowFrameBoundType extends Constant {
    /**
     * The constant OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING.
     */
    public static final OmniWindowFrameBoundType OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING = new OmniWindowFrameBoundType(0);

    /**
     * The constant OMNI_FRAME_BOUND_PRECEDING.
     */
    public static final OmniWindowFrameBoundType OMNI_FRAME_BOUND_PRECEDING = new OmniWindowFrameBoundType(1);

    /**
     * The constant OMNI_FRAME_BOUND_CURRENT_ROW.
     */
    public static final OmniWindowFrameBoundType OMNI_FRAME_BOUND_CURRENT_ROW = new OmniWindowFrameBoundType(2);

    /**
     * The constant OMNI_FRAME_BOUND_FOLLOWING.
     */
    public static final OmniWindowFrameBoundType OMNI_FRAME_BOUND_FOLLOWING = new OmniWindowFrameBoundType(3);

    /**
     * The constant OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING.
     */
    public static final OmniWindowFrameBoundType OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING = new OmniWindowFrameBoundType(4);

    private static final long serialVersionUID = 3646147886114670835L;

    /**
     * Instantiates a new frame bound type.
     *
     * @param value the value
     */
    public OmniWindowFrameBoundType(int value) {
        super(value);
    }
}
