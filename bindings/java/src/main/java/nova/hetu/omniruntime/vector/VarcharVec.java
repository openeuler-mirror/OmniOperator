package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;

public class VarcharVec
        extends VariableWidthVec
{
    private static final byte[] emptyByteArray = new byte[] {};

    private int lastOffsetPosition;
    private int capacityInBytes;

    public VarcharVec(int capacityInBytes, int size)
    {
        super(capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR);
        this.capacityInBytes = capacityInBytes;
        lastOffsetPosition = -1;
    }

    public VarcharVec(VecAllocator allocator, int capacityInBytes, int size)
    {
        super(allocator, capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR);
        lastOffsetPosition = -1;
    }

    private VarcharVec(VarcharVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    protected VarcharVec(long nativeVector)
    {
        super(nativeVector);
    }

    public byte[] getValue(int index)
    {
        checkIndex(index);
        final int actualIndex = index + getOffset();
        // check is null
        if (getValueNulls() != null && !getValueNulls().get(actualIndex)) {
            return null;
        }

        final int startOffset = getValueOffset(index);
        final int dataLen = getValueOffset(index + 1) - startOffset;
        final byte[] data = new byte[dataLen];
        getData(startOffset, data, 0, data.length);
        return data;
    }

    private void getData(int startOffset, byte[] dst, int start, int length)
    {
        if (startOffset > capacityInBytes - length) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, String.format("startOffset: %d, length: %d (expected: range(0, %d))",
                    startOffset, length, capacityInBytes));
        }
        getValues().position(startOffset);
        getValues().get(dst, start, length);
    }

    public void setValue(int index, byte[] value)
    {
        checkIndex(index);
        if (!isWritable()) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "this vector is not support written.");
        }
        final int actualIndex = index + getOffset();
        fillSlots(actualIndex);
        if (getValueNulls() != null) {
            getValueNulls().set(actualIndex);
        }
        setData(actualIndex, value, 0, value.length);
        lastOffsetPosition = actualIndex;
    }

    private void checkIndex(int index)
    {
        if (index < 0 || index >= getSize() - getOffset()) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "the actual index is not valid:index="
                    + index + ",offset=" + getOffset() + ",elementCount=" + getSize());
        }
    }

    private void fillSlots(int index)
    {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            setData(i, emptyByteArray, 0, emptyByteArray.length);
        }
        lastOffsetPosition = index - 1;
    }

    private void setData(int index, byte[] data, int start, int length)
    {
        final int startOffset = getValueOffset(index);
        if (startOffset > capacityInBytes - length) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, String.format("startOffset: %d, length: %d (expected: range(0, %d))",
                    startOffset, length, capacityInBytes));
        }
        setValueOffset(index + 1, startOffset + data.length);
        getValues().position(startOffset);
        getValues().put(data, start, length);
    }

    @Override
    public Vec slice(int start, int end)
    {
        return new VarcharVec(this, start + getOffset(), end - start);
    }

    @Override
    public Vec copy()
    {
        return null;
    }
}
