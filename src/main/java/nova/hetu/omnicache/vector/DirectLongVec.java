package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;

// TODO: for benchmarking only
public class DirectLongVec
{
    protected ByteBuffer data;
    protected int size;

    public DirectLongVec(int size)
    {
        this.size = size;
        data = ByteBuffer.allocateDirect(size * Long.BYTES);
    }

    public void set(int idx, Long value)
    {
        data.putLong(idx * Long.BYTES, value);
    }

    public Long get(int idx)
    {
        return data.getLong(idx * Long.BYTES);
    }

    public int size()
    {
        return size;
    }
}
