package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

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

    public void put(int index, int[] valueOffsets)
    {
        IntBuffer buffer = offsets.asIntBuffer();
        buffer.position(index);
        buffer.put(valueOffsets, 0, valueOffsets.length);
    }

    public void getOffsets(int index, int[] targetValueOffsets, int start, int length)
    {
        IntBuffer buffer = offsets.asIntBuffer();
        buffer.position(index);
        buffer.get(targetValueOffsets, start, length);
    }
}
