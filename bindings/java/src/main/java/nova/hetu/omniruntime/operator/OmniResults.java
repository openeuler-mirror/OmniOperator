package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.constants.Status;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.Closeable;

public class OmniResults
        implements Closeable
{
    private final VecBatch[] vecBatches;

    private final Status status;

    public OmniResults(VecBatch[] vecBatches, int status)
    {
        this.vecBatches = vecBatches;
        this.status = new Status(status);
    }

    public VecBatch[] getVecBatches()
    {
        return vecBatches;
    }

    public Status getStatus()
    {
        return status;
    }

    @Override
    public void close()
    {
        for (VecBatch vecBatch : vecBatches) {
            vecBatch.close();
        }
    }
}
