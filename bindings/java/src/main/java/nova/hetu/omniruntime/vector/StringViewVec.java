/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.StringViewDataType;

/**
 * string view vec.
 *
 * @since 2026-05-26
 */
public class StringViewVec extends Vec {
    private static final int STRING_VIEW_BYTES = 16;
    private static final int STRING_VIEW_DATA_OFFSET = Integer.BYTES;
    private static final int STRING_VIEW_POINTER_OFFSET = 2 * Integer.BYTES;
    private static final int INLINE_BYTES = 12;

    public StringViewVec(int size) {
        super(STRING_VIEW_BYTES * size, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, StringViewDataType.STRING_VIEW);
    }

    public StringViewVec(long nativeVector) {
        super(nativeVector, StringViewDataType.STRING_VIEW, STRING_VIEW_BYTES);
    }

    public StringViewVec(long nativeVector, long nativeVectorValueBufAddress, long nativeVectorNullBufAddress,
            int size) {
        super(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress, STRING_VIEW_BYTES * size, size,
                StringViewDataType.STRING_VIEW);
    }

    private StringViewVec(StringViewVec vector, int offset, int length) {
        super(vector, offset, length, STRING_VIEW_BYTES * length);
    }

    private StringViewVec(StringViewVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length, STRING_VIEW_BYTES * length);
    }

    public byte[] get(int index) {
        int baseOffset = index * STRING_VIEW_BYTES;
        int length = valuesBuf.getInt(baseOffset);
        if (length <= INLINE_BYTES) {
            return valuesBuf.getBytes(baseOffset + STRING_VIEW_DATA_OFFSET, length);
        }

        long dataAddress = valuesBuf.getLong(baseOffset + STRING_VIEW_POINTER_OFFSET);
        return OmniBufferFactory.create(dataAddress, length).getBytes(0, length);
    }

    @Override
    public StringViewVec slice(int start, int length) {
        return new StringViewVec(this, start, length);
    }

    @Override
    public StringViewVec copyPositions(int[] positions, int offset, int length) {
        return new StringViewVec(this, positions, offset, length);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return STRING_VIEW_BYTES * size;
    }
}
