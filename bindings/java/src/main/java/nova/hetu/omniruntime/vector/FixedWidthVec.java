/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

/**
 * base class of fixed width vec
 *
 * @since 2021-07-17
 */
public abstract class FixedWidthVec
        extends Vec {
    public FixedWidthVec(int capacityInBytes, int size, VecType type) {
        super(capacityInBytes, size, type);
    }

    public FixedWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type) {
        super(allocator, capacityInBytes, size, type);
    }

    public FixedWidthVec(FixedWidthVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    public FixedWidthVec(FixedWidthVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    public FixedWidthVec(long nativeVector) {
        super(nativeVector);
    }
}
