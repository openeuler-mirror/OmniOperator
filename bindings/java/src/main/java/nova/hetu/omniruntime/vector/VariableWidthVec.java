package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class VariableWidthVec
        extends Vec
{
    private final ValueOffsets valueOffsets;

    public VariableWidthVec(int capacityInBytes, int size, VecType type)
    {
        super(capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    public VariableWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type)
    {
        super(allocator, capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int offset, int length, boolean isSlice)
    {
        super(vec, offset, length, isSlice);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int[] positions, int offset, int length)
    {
        super(vec, positions, offset, length);
        this.valueOffsets = new ValueOffsets((getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN)));
    }

    protected VariableWidthVec(long nativeVector)
    {
        super(nativeVector);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected int getValueOffset(int index)
    {
        return valueOffsets.get(index);
    }

    protected void setValueOffset(int index, int offset)
    {
        valueOffsets.set(index, offset);
    }

    protected static native ByteBuffer getValueOffsetsNative(long nativeVector);
}
