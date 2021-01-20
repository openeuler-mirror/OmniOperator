package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntVec
        extends Vec<Integer>
{
    public IntVec(int size)
    {
        super(size * Integer.BYTES);
        this.size = size;
    }

    @Override
    public void set(int idx, Integer value)
    {
        data.putInt(idx * Integer.BYTES, value);
    }

    @Override
    public Integer get(int idx)
    {
        return data.getInt(idx * Integer.BYTES);
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Integer other)
    {
        base.mul(OMVectorBase.INT_DATA_TYPE, data, other.intValue());
        return this;
    }

    @Override
    public Vec mmul(Vec<Integer> other)
    {
        return null;
    }

    @Override
    public Vec filter()
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
