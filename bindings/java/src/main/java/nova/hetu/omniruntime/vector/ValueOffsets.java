package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;

public class ValueOffsets
{
    private final ByteBuffer offsets;
    private static final int STEP = Integer.BYTES;

    public ValueOffsets(ByteBuffer offsets)
    {
        this.offsets = offsets;
    }

    public void set(int index, int value)
    {
        offsets.putInt(index * STEP, value);
    }

    public int get(int index)
    {
        return offsets.getInt(index * STEP);
    }
}
