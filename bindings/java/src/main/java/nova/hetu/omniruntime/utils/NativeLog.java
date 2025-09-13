/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
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
    private static volatile NativeLog instance;

    private static final Logger logger = LoggerFactory.getLogger(NativeLog.class);

    /**
     * Native Log init.
     *
     * @since 20220320
     */
    private NativeLog() {
        initLog();
    }

    /**
     * Native getInstance.
     *
     * @return new NativeLog
     */
    public static NativeLog getInstance() {
        if (instance == null) {
            synchronized (NativeLog.class) {
                if (instance == null) {
                    instance = new NativeLog();
                }
            }
        }
        return instance;
    }

    /**
     * Init global logger.
     */
    public static native void initLog();
}