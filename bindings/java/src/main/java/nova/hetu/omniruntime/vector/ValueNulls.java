/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;

/**
 * nulls values buffer
 *
 * @since 2021-07-17
 */
public class ValueNulls {
    private final ByteBuffer bitmap;

    public ValueNulls(ByteBuffer bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * mark the specified position as a null value
     *
     * @param index the element offset in vec
     */
    public void set(int index) {
        bitmap.put(index, (byte) 1);
    }

    /**
     * mark the specified position as a non-null value
     *
     * @param index the element offset in vec
     */
    public void unset(int index) {
        bitmap.put(index, (byte) 0);
    }

    /**
     * set null values ​​in batch
     *
     * @param valueNulls value of nulls
     */
    public void set(ValueNulls valueNulls) {
        this.bitmap.put(valueNulls.bitmap);
    }

    /**
     * get the specified boolean at the specified absolute
     *
     * @param index the element offset in vec
     * @return if the value of 1 returns true, otherwise it returns false
     */
    public boolean get(int index) {
        return bitmap.get(index) == 1;
    }

    public void get(int index, boolean[] targetValueNulls, int start, int length) {
        byte[] nulls = new byte[length];
        bitmap.position(index);
        bitmap.get(nulls, 0, length);
        for (int i = 0; i < length; i++) {
            targetValueNulls[start + i] = nulls[i] == 1;
        }
    }

    public void set(int index, boolean[] isNulls, int start, int length) {
        for (int i = 0; i < length; i++) {
            if (isNulls[i + start]) {
                bitmap.put(i + index, (byte) 1);
            }
        }
    }
}
