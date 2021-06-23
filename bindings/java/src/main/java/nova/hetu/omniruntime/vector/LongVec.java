package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;

public class LongVec
        extends FixedWidthVec
{
    private static final int BYTES = Long.BYTES;

    public LongVec(int size)
    {
        super(size * BYTES, size, OMNI_VEC_TYPE_LONG);
    }

    public LongVec(VecAllocator allocator, int size)
    {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_LONG);
    }

    protected LongVec(long nativeVector)
    {
        super(nativeVector);
    }

    private LongVec(LongVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    /**
     * This constructor of vector is just for shuffle compilation to pass, it will be removed later
     * @param data data of vector
     * @param capacityInBytes size in bytes of data
     */
    @Deprecated
    public LongVec(ByteBuffer data, int capacityInBytes)
    {
        super(capacityInBytes, data.limit(), OMNI_VEC_TYPE_LONG);
    }

    public long get(int index)
    {
        return getValues().getLong((index + getOffset()) * BYTES);
    }

    public void set(int index, long value)
    {
        getValues().putLong((index + getOffset()) * BYTES, value);
    }

    public void put(long[] values, int offset, int start, int length)
    {
        LongBuffer buffer = getValues().asLongBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
    }

    @Override
    public LongVec slice(int start, int end)
    {
        return new LongVec(this, start + getOffset(), end - start);
    }

    @Override
    public LongVec copy()
    {
        return null;
    }
}
