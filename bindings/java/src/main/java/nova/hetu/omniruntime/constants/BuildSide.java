/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The Build Side.
 *
 * @since 2025-01-14
 */
public class BuildSide extends Constant {
    /**
     * The constant BUILD_UNKNOWN. The representative doesn't need to know
     */
    public static final BuildSide BUILD_UNKNOWN = new BuildSide(0);

    /**
     * The constant BUILD_LEFT.
     */
    public static final BuildSide BUILD_LEFT = new BuildSide(1);

    /**
     * The constant BUILD_RIGHT.
     */
    public static final BuildSide BUILD_RIGHT = new BuildSide(2);

    private static final long serialVersionUID = -4047841645954651422L;

    /**
     * Instantiates a new Build Side.
     *
     * @param value the value
     */
    public BuildSide(int value) {
        super(value);
    }
}
