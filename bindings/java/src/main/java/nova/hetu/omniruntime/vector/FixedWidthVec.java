package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

public abstract class FixedWidthVec
        extends Vec
{
    public FixedWidthVec(int capacityInBytes, int size, VecType type)
    {
        super(capacityInBytes, size, type);
    }

    public FixedWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type)
    {
        super(allocator, capacityInBytes, size, type);
    }

    public FixedWidthVec(FixedWidthVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    protected FixedWidthVec(long nativeVector)
    {
        super(nativeVector);
    }
}
