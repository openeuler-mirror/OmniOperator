/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * offset value buffer
 *
 * @since 2021-07-17
 */
public class ValueOffsets {
    private static final int STEP = Integer.BYTES;

    private final ByteBuffer offsets;

    public ValueOffsets(ByteBuffer offsets) {
        this.offsets = offsets;
    }

    /**
     * set the offset of the specified position
     *
     * @param index the element offset in vec
     * @param value the offset value
     */
    public void set(int index, int value) {
        offsets.putInt(index * STEP, value);
    }

    /**
     * get the offset of the specified position
     *
     * @param index the element offset in vec
     * @return the offset value
     */
    public int get(int index) {
        return offsets.getInt(index * STEP);
    }

    public void put(int index, int[] valueOffsets) {
        IntBuffer buffer = offsets.asIntBuffer();
        buffer.position(index);
        buffer.put(valueOffsets, 0, valueOffsets.length);
    }

    public void getOffsets(int index, int[] targetValueOffsets, int start, int length) {
        IntBuffer buffer = offsets.asIntBuffer();
        buffer.position(index);
        buffer.get(targetValueOffsets, start, length);
    }
}
