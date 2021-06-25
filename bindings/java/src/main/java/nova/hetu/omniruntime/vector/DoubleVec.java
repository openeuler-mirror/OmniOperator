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

    protected DoubleVec(long nativeVector)
    {
        super(nativeVector);
    }

    private DoubleVec(DoubleVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    public double get(int index)
    {
        return getValues().getDouble((index + getOffset()) * BYTES);
    }

    public void set(int index, double value)
    {
        getValues().putDouble((index + getOffset()) * BYTES, value);
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
        return new DoubleVec(this, start + getOffset(), end - start);
    }

    @Override
    public DoubleVec copy()
    {
        return null;
    }
}
