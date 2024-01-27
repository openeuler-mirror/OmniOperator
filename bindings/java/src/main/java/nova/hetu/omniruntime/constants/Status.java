/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

/**
 * The type Status.
 *
 * @since 2021-06-30
 */
public class Status extends Constant {
    /**
     * The constant OMNI_STATUS_NORMAL.
     */
    public static final Status OMNI_STATUS_NORMAL = new Status(0);

    /**
     * The constant OMNI_STATUS_ERROR.
     */
    public static final Status OMNI_STATUS_ERROR = new Status(-1);

    /**
     * The constant OMNI_STATUS_FINISHED.
     */
    public static final Status OMNI_STATUS_FINISHED = new Status(1);

    private static final long serialVersionUID = -3424552555224669902L;

    /**
     * Instantiates a new Status.
     *
     * @param value the value
     */
    public Status(int value) {
        super(value);
    }
}
