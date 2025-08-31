/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

/**
 * base class of complex vec.
 *
 * @since 2025-08-29
 */
public abstract class ComplexVec extends Vec {

    public ComplexVec(long nativeVector, int capacityInBytes, int size, DataType dataType) {
        super(nativeVector, 0, getValueNullsNative(nativeVector), capacityInBytes, size,
            dataType);
    }

    protected static native int getComplexCapacityNative(long nativeVector, int vecEncodingId);

    protected static native long newComplexVectorNative(int size, int vecEncodingId, DataType[] dataType);

    protected static native long newEmptyComplexVectorNative(int size, int vecEncodingId, DataType[] dataType);
}



