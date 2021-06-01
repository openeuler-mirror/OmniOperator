package nova.hetu.omniruntime.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class VecBatch
{
    private final Vec[] vectors;

    private final int rowCount;

    public VecBatch(Vec[] vectors, int rowCount)
    {
        this.vectors = vectors;
        this.rowCount = rowCount;
    }

    public VecBatch(List<Vec> vectors, int rowCount)
    {
        this.vectors = vectors.toArray(new Vec[vectors.size()]);
        this.rowCount = rowCount;
    }

    public VecBatch(ByteBuffer[] buffers, int[] types, int rowCount)
    {
        vectors = new Vec[buffers.length];
        for (int idx = 0; idx < types.length; idx++) {
            //TODO: Need Byte Order Configurable
            ByteBuffer buffer = buffers[idx];
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            switch (types[idx]) {
                case 1:
                    vectors[idx] = new IntVec(buffer, rowCount);
                    break;
                case 2:
                    vectors[idx] = new LongVec(buffer, rowCount);
                    break;
                case 3:
                    vectors[idx] = new DoubleVec(buffer, rowCount);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Not Support Vec Type %s", types[idx]));
            }
        }
        this.rowCount = rowCount;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public Vec[] getVectors()
    {
        return vectors;
    }

    public void close()
    {
        for (Vec vector : vectors) {
            vector.close();
        }
    }
}
