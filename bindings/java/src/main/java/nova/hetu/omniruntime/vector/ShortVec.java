package nova.hetu.omniruntime.vector;

import java.nio.ShortBuffer;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;

public class ShortVec
        extends FixedWidthVec
{
    private static final int BYTES = Short.BYTES;

    public ShortVec(int size)
    {
        super(size * BYTES, size, OMNI_VEC_TYPE_LONG);
    }

    public ShortVec(VecAllocator allocator, int size)
    {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_LONG);
    }

    protected ShortVec(long nativeVector)
    {
        super(nativeVector);
    }

    private ShortVec(ShortVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    public short get(int index)
    {
        return getValues().getShort((index + getOffset()) * BYTES);
    }

    public void set(int index, short value)
    {
        getValues().putShort((index + getOffset()) * BYTES, value);
    }

    public void put(short[] values, int offset, int start, int length)
    {
        ShortBuffer buffer = getValues().asShortBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
    }

    @Override
    public ShortVec slice(int start, int end)
    {
        return new ShortVec(this, start + getOffset(), end - start);
    }

    @Override
    public ShortVec copy()
    {
        return null;
    }
}
