package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

import java.nio.ByteBuffer;

public abstract class VariableWidthVec
        extends Vec
{
    private final ValueOffsets valueOffsets;

    public VariableWidthVec(int capacityInBytes, int size, VecType type)
    {
        super(capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()));
    }

    public VariableWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type)
    {
        super(allocator, capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()));
    }

    protected VariableWidthVec(Vec vec, int offset, int length)
    {
        super(vec, offset, length);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()));
    }

    protected VariableWidthVec(long nativeVector)
    {
        super(nativeVector);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()));
    }

    public int getValueOffset(int index)
    {
        return valueOffsets.get(index);
    }

    public void setValueOffset(int index, int offset)
    {
        valueOffsets.set(index, offset);
    }

    protected static native ByteBuffer getValueOffsetsNative(long nativeVector);
}
