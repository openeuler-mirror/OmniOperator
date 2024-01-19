/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

/**
 * Responsible for creating different type of omniBuffer.
 *
 * @since 2021-08-10
 */
public class OmniBufferFactory {
    private OmniBufferFactory() {
    }

    /**
     * create a new omnibuffer object.
     *
     * @param address the address of buffer object
     * @param capacity the capacity of buffer object
     * @return omnibuffer object
     */
    public static OmniBuffer create(long address, int capacity) {
        return new OmniBufferUnsafeV8(address, capacity);
    }
}
