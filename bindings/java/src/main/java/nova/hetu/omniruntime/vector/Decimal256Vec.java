/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.math.BigDecimal;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_256_DECIMAL;

/**
 * 256-bit decimal vec
 *
 * @since 2021-07-17
 */
public class Decimal256Vec
        extends DecimalVec {
    private static final int BYTES = Long.BYTES * 4;

    public Decimal256Vec(int size, int precision, int scale) {
        super(size, precision, scale, BYTES, OMNI_VEC_TYPE_256_DECIMAL);
    }

    public Decimal256Vec(VecAllocator allocator, int size, int precision, int scale) {
        super(allocator, size, precision, scale, BYTES, OMNI_VEC_TYPE_256_DECIMAL);
    }

    public Decimal256Vec(long nativeVector) {
        super(nativeVector);
    }

    private Decimal256Vec(Decimal256Vec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private Decimal256Vec(Decimal256Vec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    @Override
    public void set(int index, BigDecimal value) {
        set(index, value, BYTES);
    }

    @Override
    public BigDecimal get(int index) {
        return get(index, BYTES);
    }

    @Override
    public Decimal256Vec slice(int start, int end) {
        return new Decimal256Vec(this, start, end - start, true);
    }

    @Override
    public Decimal256Vec copy() {
        throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Decimal256Vec is not supported");
    }

    @Override
    public Decimal256Vec copyPositions(int[] positions, int offset, int length) {
        return new Decimal256Vec(this, positions, offset, length);
    }

    @Override
    public Decimal256Vec copyRegion(int positionOffset, int length) {
        return new Decimal256Vec(this, positionOffset, length, false);
    }
}
