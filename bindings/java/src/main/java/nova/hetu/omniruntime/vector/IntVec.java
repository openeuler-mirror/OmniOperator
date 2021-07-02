package nova.hetu.omniruntime.vector;

import java.nio.IntBuffer;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;

public class IntVec
        extends FixedWidthVec
{
    private static final int BYTES = Integer.BYTES;

    public IntVec(int size)
    {
        super(size * BYTES, size, OMNI_VEC_TYPE_INT);
    }

    public IntVec(VecAllocator allocator, int size)
    {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_INT);
    }

    protected IntVec(long nativeVector)
    {
        super(nativeVector);
    }

    private IntVec(IntVec vector, int offset, int length, boolean isSlice)
    {
        super(vector, offset, length, isSlice);
    }

    private IntVec(IntVec vector, int[] positions, int offset, int length)
    {
        super(vector, positions, offset, length);
    }

    public int get(int index)
    {
        return getValues().getInt((index + offset) * BYTES);
    }

    public void set(int index, int value)
    {
        getValues().putInt(index * BYTES, value);
    }

    public void put(int[] values, int offset, int start, int length)
    {
        IntBuffer buffer = getValues().asIntBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
    }

    @Override
    public IntVec slice(int start, int end)
    {
        return new IntVec(this, start, end - start, true);
    }

    @Override
    public IntVec copy()
    {
        return null;
    }

    @Override
    public IntVec copyPositions(int[] positions, int offset, int length)
    {
        return new IntVec(this, positions, offset, length);
    }

    @Override
    public IntVec copyRegion(int positionOffset, int length)
    {
        return new IntVec(this, positionOffset, length, false);
    }
}
