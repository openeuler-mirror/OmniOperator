/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime;

import nova.hetu.omniruntime.utils.NativeLog;

/**
 * common static load logic
 *
 * @since 2024-01-18
 */
public class CommonStaticLoad {
    /**
     * static load
     */
    public static void load() {
        OmniLibs.load();
        NativeLog.getInstance();
    }
}
