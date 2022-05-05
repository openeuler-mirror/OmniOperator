/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The global log init.
 *
 * @since 20220320
 */

public class NativeLog {
    private static final Logger logger = LoggerFactory.getLogger(NativeLog.class);

    /**
     * Native Log init.
     *
     *  @since 20220320
     */
    private NativeLog() {
        initLog();
    }

    /**
     * The global native log holder init.
     *
     * @since 20220320
     */
    private static class NativeLogHolder {
        static final NativeLog INSTANCE = new NativeLog();
    }

    /**
     * Native getInstance.
     *
     * @return new NativeLog
     */
    public static NativeLog getInstance() {
        return NativeLogHolder.INSTANCE;
    }

    /**
     * Init global logger.
     *
     */
    public static native void initLog();
}