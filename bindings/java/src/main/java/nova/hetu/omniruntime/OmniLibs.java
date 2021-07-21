/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime;

/**
 * load libomruntime.so
 *
 * @since 2021-07-17
 */
public class OmniLibs
{
    private static final String OMNI_RUNTIME = "omruntime";

    private OmniLibs() {
    }

    public static void load() {
        System.loadLibrary(OMNI_RUNTIME);
    }
}
