package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;

public class IntVector
        extends Vec<Integer>
{
    private ByteBuffer data;

    public IntVector(int size)
    {
        data = OMVectorBase.allocate(size * Integer.BYTES);
    }

    @Override
    public void set(int idx, Integer value)
    {

    }

    @Override
    public Integer get(int idx)
    {
        return null;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Vec<Integer> other)
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
