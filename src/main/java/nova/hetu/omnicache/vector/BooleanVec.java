package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;

public class BooleanVec
        extends Vec
{
    public BooleanVec(int size)
    {
        super(size, size);
    }

    public BooleanVec(ByteBuffer buffer, int length)
    {
        super(buffer, length);
    }

    public void set(int idx)
    {
        this.getData().put(idx, (byte) 1);
    }

    public void unset(int idx)
    {
        this.getData().put(idx, (byte) 0);
    }

    @Override
    public BooleanVec slice(int startIdx, int endIdx)
    {
        byte[] regionData = new byte[endIdx - startIdx];
        BooleanVec newVec = new BooleanVec(endIdx - startIdx);
        this.getData().position(startIdx);
        this.getData().get(regionData, 0, regionData.length);
        newVec.getData().put(regionData);
        return newVec;
    }

    public int get(int idx)
    {
        return this.getData().get(idx);
    }

    public boolean isSet(int idx)
    {
        return this.getData().get(idx) != 0;
    }

    @Override
    public VecType getType()
    {
        return VecType.BOOLEAN;
    }
}
