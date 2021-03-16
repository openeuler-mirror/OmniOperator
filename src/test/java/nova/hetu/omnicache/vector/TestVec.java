package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;

public class TestVec
        extends Vec
{

    public TestVec(int rowSize, int alloc_size)
    {
        super(rowSize, alloc_size);
    }

    public TestVec(ByteBuffer data, int length)
    {
        super(data, length);
    }

    @Override
    public Vec slice(int startIdx, int endIdx)
    {
        return null;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(int other)
    {
        return null;
    }

    @Override
    public Vec mmul(Vec other)
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

    @Override
    public Vec concat(Vec other)
    {
        return null;
    }

    @Override
    public VecType getType()
    {
        return null;
    }
}
