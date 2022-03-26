/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

/**
 * Responsible for creating different type of omniBuf.
 *
 * @since 2021-08-10
 */
public class OmniBufFactory {
    private OmniBufFactory() {
    }

    /**
     * create a new omnibuf object.
     *
     * @param address the address of buffer object
     * @param capacity the capacity of buffer object
     * @return omnibuf object
     */
    public static OmniBuf create(long address, int capacity) {
        return new OmniBufUnsafeV8(address, capacity);
    }
}
