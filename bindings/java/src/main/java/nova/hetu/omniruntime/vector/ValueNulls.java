package nova.hetu.omniruntime.vector;

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
        bitmap.put(index, (byte) 1);
    }

    public void unset(int index)
    {
        bitmap.put(index, (byte) 0);
    }

    public void set(ValueNulls valueNulls)
    {
        this.bitmap.put(valueNulls.bitmap);
    }

    public boolean get(int index)
    {
        return bitmap.get(index) == 1;
    }
}
