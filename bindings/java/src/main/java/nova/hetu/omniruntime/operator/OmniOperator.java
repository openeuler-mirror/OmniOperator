/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_NORMAL;

import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;

/**
 * The type Omni operator.
 *
 * @since 20210630
 */
public final class OmniOperator implements AutoCloseable {
    /**
     * The Native operator.
     */
    protected final long nativeOperator;

    private final VecAllocator vecAllocator;

    private VecBatchIterator outputIterator;

    /**
     * Instantiates a new Omni operator.
     *
     * @param nativeOperator the native operator
     * @param vecAllocator vector allocator
     */
    protected OmniOperator(long nativeOperator, VecAllocator vecAllocator) {
        this.nativeOperator = nativeOperator;
        this.vecAllocator = vecAllocator;
    }

    // addInput
    private static native int addInputNative(long nativeOperator, long nativeVectorBatch);

    // getOutput
    private static native OmniResults getOutputNative(long nativeOperator);

    // close
    private static native void closeNative(long nativeOperator);

    /**
     * Add input int.
     *
     * @param vecBatch the vec batch
     * @return the int
     */
    public int addInput(VecBatch vecBatch) {
        return addInputNative(nativeOperator, vecBatch.getNativeVectorBatch());
    }

    /**
     * Gets output.
     *
     * @return the output
     */
    public Iterator<VecBatch> getOutput() {
        if (outputIterator == null) {
            outputIterator = new VecBatchIterator();
        }
        return outputIterator;
    }

    public void close() {
        closeNative(nativeOperator);
    }

    /**
     * Gets vec allocator.
     *
     * @return the vec allocator
     */
    public VecAllocator getVecAllocator() {
        return vecAllocator;
    }

    private class VecBatchIterator implements Iterator<VecBatch> {
        private OmniResults results;

        private int index;

        /**
         * Instantiates a new Vec batch iterator.
         */
        public VecBatchIterator() {
            resetIterator();
            advanced();
        }

        @Override
        public boolean hasNext() {
            // if it first, the results is null,
            // or index reach the count of vector batches but it don't finished,
            // then advanced().
            if (results == null || (index == results.getVecBatches().length && !isFinished())) {
                resetIterator();
                advanced();
            }

            // after advanced(), if results is still null,
            // means there is no more data.
            if (results == null) {
                return false;
            }

            // if index reach the count of vector batches
            // means it finished, return and clean the context.
            if (index == results.getVecBatches().length) {
                resetIterator();
                return false;
            }
            return true;
        }

        @Override
        public VecBatch next() {
            return results.getVecBatches()[index++];
        }

        private void resetIterator() {
            results = null;
            index = 0;
        }

        private void advanced() {
            results = getOutputNative(nativeOperator);
        }

        private boolean isFinished() {
            // TODO: Handle error.
            return !OMNI_STATUS_NORMAL.equals(results.getStatus());
        }
    }
}
