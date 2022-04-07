/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import static java.lang.Math.min;
import static java.lang.System.lineSeparator;

import java.util.Arrays;
import java.util.StringJoiner;

/**
 * trace util.
 *
 * @since 2021-10-21
 */
public class TraceUtil {
    /**
     * used for get stack trace of current thread.
     *
     * @return the stack trace of current thread
     */
    public static String stack() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        StringJoiner stack = new StringJoiner(lineSeparator() + "\t");
        Arrays.stream(elements).skip(2).limit(min(25, elements.length - 1)).forEach(item -> {
            stack.add(item.toString());
        });
        return stack.toString();
    }
}
