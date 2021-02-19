package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;

public class VarcharVec extends VarVec
{
    private static final int BUFFER_MAX_SIZE = 300*1024;
    public VarcharVec(long capacity, int elements)
    {
        super(capacity, elements);
    }

    public VarcharVec(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public Vec slice(int startIdx, int endIdx)
    {
        return null;
    }

    public void set(int idx, int offset, int length)
    {
        this.offsets[idx] = offset;
        this.lengths[idx] = length;
    }

    public void setData(byte[] data)
    {
        for (int i=0; i<this.buffers.length; i++) {
            this.buffers[i].put(data, i* MAX_BUFFER_SIZE, MAX_BUFFER_SIZE);
        }
    }

    public ByteBuffer getData(int idx)
    {
        if (lengths[idx] == 0) {
            return ByteBuffer.wrap("".getBytes());
        } else {
            byte[] output = new byte[lengths[idx]];
            int length = lengths[idx];
            int offset = offsets[idx];
            int bufferPosition = offset / MAX_BUFFER_SIZE;
            int bufferOffset = offset % MAX_BUFFER_SIZE;
            this.buffers[bufferPosition].position(bufferOffset);
            return this.buffers[bufferPosition].get(output, 0, length);
        }
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
    public Vec concat(Vec other)
    {
        return null;
    }

    @Override
    public Vec join(Vec other)
    {
        return null;
    }

    @Override
    public VecType getType()
    {
        return VecType.DOUBLE;
    }

    @Override
    public ByteBuffer getData()
    {
        return null;
    }
}