package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.Closeable;
import java.util.Iterator;

import static nova.hetu.omniruntime.operator.OmniResults.Status.NORMAL;

public final class OmniOperator
        implements Closeable
{
    protected final long nativeOperator;

    private VecBatchIterator outputIterator = new VecBatchIterator();

    protected OmniOperator(long nativeOperator)
    {
        this.nativeOperator = nativeOperator;
    }

    public int addInput(ImmutableList<VecBatch> vecBatches)
    {
        if (vecBatches.isEmpty()) {
            return 0;
        }
        int totalVectorCount = vecBatches.size() * vecBatches.get(0).getVectors().length;
        LongVec addressVector = new LongVec(totalVectorCount);
        IntVec rowCountVector = new IntVec(vecBatches.size());
        try {
            for (int batchIdx = 0; batchIdx < vecBatches.size(); batchIdx++) {
                Vec[] vectors = vecBatches.get(batchIdx).getVectors();
                int vecCount = vectors.length;
                for (int vecIdx = 0; vecIdx < vecCount; vecIdx++) {
                    int index = batchIdx * vecCount + vecIdx;
                    addressVector.set(index, vectors[vecIdx].getAddress());
                }
                rowCountVector.set(batchIdx, vecBatches.get(batchIdx).getRowCount());
            }
            addInput(nativeOperator, addressVector.getAddress(), totalVectorCount, rowCountVector.getAddress(), vecBatches.size());
            return 0;
        }
        finally {
            addressVector.close();
            rowCountVector.close();
        }
    }

    public Iterator<VecBatch> getOutput()
    {
        return outputIterator;
    }

    public void close()
    {
        // TODO: free output vectory
        closeNative(nativeOperator);
    }

    private class VecBatchIterator
            implements Iterator<VecBatch>
    {
        private OmniResults results;

        private int index = 0;

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
            results = getOutput(nativeOperator);
        }

        private boolean isFinished()
        {
            // TODO: Handle error.
            return results.getStatus() != NORMAL;
        }
    }

    // addInput
    private static native int addInput(long operatorAddress, long vecAddress, int vecNum, long rowCountAddress, int rowCountNum);

    // getOutput
    private static native OmniResults getOutput(long nativeOperator);

    // close
    private static native void closeNative(long nativeOperator);
}
