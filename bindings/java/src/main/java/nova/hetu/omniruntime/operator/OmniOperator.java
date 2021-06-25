package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;

import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_NORMAL;

public final class OmniOperator
        implements AutoCloseable
{
    protected final long nativeOperator;

    private VecBatchIterator outputIterator;

    protected OmniOperator(long nativeOperator)
    {
        this.nativeOperator = nativeOperator;
    }

    public int addInput(VecBatch vecBatch)
    {
        return addInputNative(nativeOperator, vecBatch.getNativeVectorBatch());
    }

    public Iterator<VecBatch> getOutput()
    {
        if (outputIterator == null) {
            outputIterator = new VecBatchIterator();
        }
        return outputIterator;
    }

    public void close()
    {
        closeNative(nativeOperator);
    }

    private class VecBatchIterator
            implements Iterator<VecBatch>
    {
        private OmniResults results;

        private int index;

        public VecBatchIterator()
        {
            advanced();
        }

        @Override
        public boolean hasNext()
        {
            if (results == null || (index == results.getVecBatches().length && !isFinished())) {
                advanced();
            }
            if (results == null || index == results.getVecBatches().length) {
                return false;
            }
            return true;
        }

        @Override
        public VecBatch next()
        {
            return results.getVecBatches()[index++];
        }

        private void advanced()
        {
            index = 0;
            results = getOutputNative(nativeOperator);
        }

        private boolean isFinished()
        {
            // TODO: Handle error.
            return !OMNI_STATUS_NORMAL.equals(results.getStatus());
        }
    }

    // addInput
    private static native int addInputNative(long nativeOperator, long nativeVectorBatch);

    // getOutput
    private static native OmniResults getOutputNative(long nativeOperator);

    // close
    private static native void closeNative(long nativeOperator);
}
