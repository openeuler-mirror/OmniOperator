package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;

public class LongVec
        extends Vec<Long>
{
    private ByteBuffer data;

    public LongVec(int size) {
        data = OMVectorBase.allocate(size * Double.BYTES);
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Vec<Long> other)
    {
        return null;
    }

    @Override
    public Vec filter()
    {
        return null;
    }

    @Override
    public void set(int idx, Long value)
    {

    }

    @Override
    public Long get(int idx)
    {
        return null;
    }

    @Override
    public Vec groupby()
    {
        return null;
    }

    @Override
    public Vec join(Vec other)
    {
        return null;
    }
}
