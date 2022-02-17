/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

/**
 * 128-bit decimal vec
 *
 * @since 2021-07-17
 */
public class Decimal128Vec extends DecimalVec {
    private static final int BYTES = Long.BYTES * 2;

    public Decimal128Vec(int size) {
        super(size, BYTES, Decimal128DataType.DECIMAL128);
    }

    public Decimal128Vec(VecAllocator allocator, int size) {
        super(allocator, size, BYTES, Decimal128DataType.DECIMAL128);
    }

    public Decimal128Vec(long nativeVector, DataType type) {
        super(nativeVector, BYTES, type);
    }

    public Decimal128Vec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
                         long nativeVectorAllocator, int capacityInBytes, int size, int offset, DataType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
            size, offset, BYTES, type);
    }

    private Decimal128Vec(Decimal128Vec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private Decimal128Vec(Decimal128Vec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * split a vec into two vec according to the specified index and length
     *
     * @param start starting index
     * @param end   ending index
     * @return new vec
     */
    @Override
    public Decimal128Vec slice(int start, int end) {
        return new Decimal128Vec(this, start, end - start, true);
    }

    /**
     * copy a new vec according to the vec
     *
     * @return new vec
     */
    @Override
    public Decimal128Vec copy() {
        throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Decimal128Vec is not supported");
    }

    /**
     * copy a new vec based on the positions
     *
     * @param positions all positions in vec
     * @param offset    position offset
     * @param length    the number of elements to be copied
     * @return new vec
     */
    @Override
    public Decimal128Vec copyPositions(int[] positions, int offset, int length) {
        return new Decimal128Vec(this, positions, offset, length);
    }

    /**
     * copy a vec based on the starting position and the number of elements
     *
     * @param positionOffset staring position
     * @param length         the number of elements
     * @return new vec
     */
    @Override
    public Decimal128Vec copyRegion(int positionOffset, int length) {
        return new Decimal128Vec(this, positionOffset, length, false);
    }
}
