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

    public void get(int index, boolean[] targetValueNulls, int start, int length)
    {
        byte[] nulls = new byte[length];
        bitmap.position(index);
        bitmap.get(nulls, 0, length);
        for (int i = 0; i < length; i++) {
            targetValueNulls[start + i] = nulls[i] == 1;
        }
    }

    public void set(int index, boolean[] isNulls, int start, int length)
    {
        for (int i = 0; i < length; i++) {
            if (isNulls[i + start]) {
                bitmap.put(i + index, (byte) 1);
            }
        }
    }
}
