package nova.hetu.omniruntime.vector;

import java.nio.DoubleBuffer;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;

public class DoubleVec
        extends FixedWidthVec
{
    private static final int BYTES = Double.BYTES;

    public DoubleVec(int size)
    {
        super(size * BYTES, size, OMNI_VEC_TYPE_DOUBLE);
    }

    public DoubleVec(VecAllocator allocator, int size)
    {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_DOUBLE);
    }

    public DoubleVec(long nativeVector)
    {
        super(nativeVector);
    }

    private DoubleVec(DoubleVec vector, int offset, int length, boolean isSlice)
    {
        super(vector, offset, length, isSlice);
    }

    private DoubleVec(DoubleVec vector, int[] positions, int offset, int length)
    {
        super(vector, positions, offset, length);
    }

    public double get(int index)
    {
        return getValues().getDouble((index + offset) * BYTES);
    }

    public void set(int index, double value)
    {
        getValues().putDouble(index * BYTES, value);
    }

    public void put(double[] values, int offset, int start, int length)
    {
        DoubleBuffer buffer = getValues().asDoubleBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
    }

    @Override
    public DoubleVec slice(int start, int end)
    {
        return new DoubleVec(this, start, end - start, true);
    }

    @Override
    public DoubleVec copy()
    {
        return null;
    }

    @Override
    public DoubleVec copyPositions(int[] positions, int offset, int length)
    {
        return new DoubleVec(this, positions, offset, length);
    }

    @Override
    public DoubleVec copyRegion(int positionOffset, int length)
    {
        return new DoubleVec(this, positionOffset, length, false);
    }
}
