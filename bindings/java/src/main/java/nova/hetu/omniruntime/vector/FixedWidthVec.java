/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

/**
 * base class of fixed width vec
 *
 * @since 2021-07-17
 */
public abstract class FixedWidthVec extends Vec {
    public FixedWidthVec(int capacityInBytes, int size, VecEncoding encoding, DataType type) {
        super(capacityInBytes, size, encoding, type);
    }

    public FixedWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecEncoding encoding, DataType type) {
        super(allocator, capacityInBytes, size, encoding, type);
    }

    public FixedWidthVec(FixedWidthVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    public FixedWidthVec(FixedWidthVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    public FixedWidthVec(long nativeVector, DataType type) {
        super(nativeVector, type);
    }

    public FixedWidthVec(long nativeVector, long nativeVectorValueBufAddress, long nativeVectorNullBufAddress,
                         long nativeVectorAllocator, int capacityInBytes, int size, int offset, DataType type) {
        super(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator,
            capacityInBytes, size, offset, type);
    }
}
