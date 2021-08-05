/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Window function type.
 *
 * @since 20210630
 */
@SuppressWarnings("StaticVariableName")
public class WindowFunctionType extends Constant {
    /**
     * The constant WIN_ROW_NUMBER.
     */
    public static WindowFunctionType WIN_ROW_NUMBER = new WindowFunctionType(0);

    /**
     * The constant WIN_RANK.
     */
    public static WindowFunctionType WIN_RANK = new WindowFunctionType(1);

    /**
     * The constant WIN_SUM.
     */
    public static WindowFunctionType WIN_SUM = new WindowFunctionType(2);

    /**
     * The constant WIN_COUNT.
     */
    public static WindowFunctionType WIN_COUNT = new WindowFunctionType(3);

    /**
     * The constant WIN_AVG.
     */
    public static WindowFunctionType WIN_AVG = new WindowFunctionType(4);

    /**
     * The constant WIN_MAX.
     */
    public static WindowFunctionType WIN_MAX = new WindowFunctionType(5);

    /**
     * The constant WIN_MIN.
     */
    public static WindowFunctionType WIN_MIN = new WindowFunctionType(6);

    /**
     * Instantiates a new Window function type.
     *
     * @param value the value
     */
    @JsonCreator
    public WindowFunctionType(@JsonProperty("value") int value) {
        super(value);
    }
}
