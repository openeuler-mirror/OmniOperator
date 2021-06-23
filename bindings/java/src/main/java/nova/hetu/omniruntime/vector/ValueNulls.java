package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.BitMapHelper;

import java.nio.ByteBuffer;

public class ValueNulls
{
    private final ByteBuffer bitmap;

    public ValueNulls(ByteBuffer bitmap)
    {
        this.bitmap = bitmap;
    }

    public void set(int index)
    {
        BitMapHelper.set(bitmap, index);
    }

    public void unset(int index)
    {
        BitMapHelper.unset(bitmap, index);
    }

    public void set(ValueNulls valueNulls)
    {
        this.bitmap.put(valueNulls.bitmap);
    }

    public boolean get(int index)
    {
        return BitMapHelper.get(bitmap, index) == 1;
    }
}
