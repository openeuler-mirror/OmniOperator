/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type window frame type.
 *
 * @since 2022-04-15
 */
public class OmniWindowFrameType extends Constant {
    /**
     * The constant OMNI_FRAME_TYPE_RANGE.
     */
    public static final OmniWindowFrameType OMNI_FRAME_TYPE_RANGE = new OmniWindowFrameType(0);

    /**
     * The constant OMNI_FRAME_TYPE_ROWS.
     */
    public static final OmniWindowFrameType OMNI_FRAME_TYPE_ROWS = new OmniWindowFrameType(1);

    private static final long serialVersionUID = -3453499979001168717L;

    /**
     * Instantiates a new window frame type.
     *
     * @param value the value
     */
    public OmniWindowFrameType(int value) {
        super(value);
    }
}
