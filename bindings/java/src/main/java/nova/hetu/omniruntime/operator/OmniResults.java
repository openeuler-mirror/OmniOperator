package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.VecBatch;

public class OmniResults
{
    private final VecBatch[] vecBatches;

    private final Status status;

    public OmniResults(VecBatch[] vecBatches, int status)
    {
        this.vecBatches = vecBatches;
        this.status = Status.values()[status];
    }

    public VecBatch[] getVecBatches()
    {
        return vecBatches;
    }

    public Status getStatus()
    {
        return status;
    }

    public enum Status
    {
        NORMAL,
        ERROR,
        FINISHED,
    }
}
