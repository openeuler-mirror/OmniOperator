/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime;

/**
 * load libomni_runtime.so.
 *
 * @since 2021-07-17
 */
public class OmniLibs {
    private static final String OMNI_RUNTIME = "boostkit-omniop-runtime-1.0.0-aarch64";

    private OmniLibs() {
    }

    /**
     * Loading the dll.
     */
    public static void load() {
        System.loadLibrary(OMNI_RUNTIME);
    }

    /**
     * Geting the version
     *
     * @return the version string
     */
    public static native String getVersion();
}
