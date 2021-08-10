/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;

/**
 * Responsible for creating different type of omniBuf
 *
 * @since 2021-08-10
 */
public class OmniBufFactory {
    private OmniBufFactory() {
    }

    /**
     * create a new omnibuf object
     *
     * @param buffer buffer object
     * @return omnibuf object
     */
    public static OmniBuf create(ByteBuffer buffer) {
        // todo:: version above jdk8 need to be considered
        return new OmniBufUnsafeV8(buffer);
    }
}
