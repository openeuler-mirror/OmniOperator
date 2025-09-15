/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */

package nova.hetu.omniruntime;

import nova.hetu.omniruntime.utils.NativeLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * load libomni_runtime.so.
 *
 * @since 2021-07-17
 */
public class OmniLibs {
    private static volatile OmniLibs instance;

    private static final String LIBRARY_NAME = "boostkit-omniop-java-binding-1.9.0-aarch64";

    private static final Logger LOG = LoggerFactory.getLogger(OmniLibs.class);

    private static final int BUFFER_SIZE = 1024;

    private OmniLibs() {
        File tempFile = null;
        try {
            String nativeLibraryPath = File.separator + System.mapLibraryName(LIBRARY_NAME);
            tempFile = File.createTempFile(LIBRARY_NAME, ".so");
            try (InputStream in = OmniLibs.class.getResourceAsStream(nativeLibraryPath);
                FileOutputStream fos = new FileOutputStream(tempFile)) {
                int i;
                byte[] buf = new byte[BUFFER_SIZE];
                while ((i = in.read(buf)) != -1) {
                    fos.write(buf, 0, i);
                }
                System.load(tempFile.getCanonicalPath());
            }
        } catch (IOException e) {
            LOG.warn("fail to load library from Jar!errmsg:{}", e.getMessage());
        } finally {
            if (tempFile != null) {
                tempFile.deleteOnExit();
            }
        }
    }

    public static OmniLibs getInstance() {
        if (instance == null) {
            synchronized (OmniLibs.class) {
                if (instance == null) {
                    instance = new OmniLibs();
                    NativeLog.getInstance();
                }
            }
        }
        return instance;
    }

    /**
     * Loading the dll.
     */
    public static void load() {
        getInstance();
    }

    /**
     * Geting the version
     *
     * @return the version string
     */
    public static native String getVersion();
}
