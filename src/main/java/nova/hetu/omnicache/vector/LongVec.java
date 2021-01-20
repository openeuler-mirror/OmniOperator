package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;

public class LongVec
        extends Vec<Long>
{

    public LongVec(int size) {
        super(size * Long.BYTES);
        this.size = size;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Long other)
    {
        base.mul(OMVectorBase.LONG_DATA_TYPE, data, other.intValue());
        return null;
    }

    @Override
    public Vec mmul(Vec<Long> other)
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
        data.putLong(idx * Long.BYTES, value);
    }

    @Override
    public Long get(int idx)
    {
        return data.getLong(idx * Long.BYTES);
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
