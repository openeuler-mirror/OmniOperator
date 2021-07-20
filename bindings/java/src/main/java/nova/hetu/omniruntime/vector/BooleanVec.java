package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_BOOLEAN;

public class BooleanVec
        extends FixedWidthVec
{
    private static final int BYTES = 1;

    public BooleanVec(int size)
    {
        super(size * BYTES, size, OMNI_VEC_TYPE_BOOLEAN);
    }

    public BooleanVec(VecAllocator allocator, int size)
    {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_BOOLEAN);
    }

    protected BooleanVec(long nativeVector)
    {
        super(nativeVector);
    }

    private BooleanVec(BooleanVec vector, int offset, int length, boolean isSlice)
    {
        super(vector, offset, length, isSlice);
    }

    private BooleanVec(BooleanVec vector, int[] positions, int offset, int length)
    {
        super(vector, positions, offset, length);
    }

    public void set(int index, boolean value)
    {
        if (value) {
            values.put(index, (byte) 1);
        }
        else {
            values.put(index, (byte) 0);
        }
    }

    public boolean get(int index)
    {
        return values.get((index + offset) * BYTES) == 1;
    }

    public void put(boolean[] values, int offset, int start, int length)
    {
        for (int i = 0; i < length; i++) {
            set(i + offset, values[i + start]);
        }
    }

    @Override
    public BooleanVec slice(int startIdx, int endIdx)
    {
        return new BooleanVec(this, startIdx, endIdx - startIdx, true);
    }

    @Override
    public Vec copy()
    {
        return null;
    }

    @Override
    public BooleanVec copyPositions(int[] positions, int offset, int length)
    {
        return new BooleanVec(this, positions, offset, length);
    }

    @Override
    public BooleanVec copyRegion(int positionOffset, int length)
    {
        return new BooleanVec(this, positionOffset, length, false);
    }
}
