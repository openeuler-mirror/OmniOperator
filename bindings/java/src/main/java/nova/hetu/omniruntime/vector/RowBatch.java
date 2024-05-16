package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.util.concurrent.atomic.AtomicBoolean;

public class RowBatch implements AutoCloseable {
    private Row[] rows;

    private int rowCount;

    protected final long nativeRowBatch;

    private AtomicBoolean isClosed = new AtomicBoolean(false);
    // only release rowBatch
    public static native void freeRowBatchNative(long nativeVectorBatch);

    public RowBatch(long nativeAddress, Row[] rows, int rowCount) {
        this.rows = rows;
        this.rowCount = rowCount;

        this.nativeRowBatch = nativeAddress;
    }

    public RowBatch(VecBatch vb) {
        RowBatch rb = transFromVectorBatch(vb.getNativeVectorBatch());
        this.rows = rb.rows;
        this.rowCount = rb.rowCount;
        this.nativeRowBatch = rb.nativeRowBatch;
    }

    public RowBatch(Row[] rows, int rowCount) {
        this.rows = rows;
        this.rowCount = rowCount;

        this.nativeRowBatch = newRowBatchNative(rows, rowCount);
    }

    public static native long newRowBatchNative(Row[] rows, int rowCount);

    public static native RowBatch transFromVectorBatch(long vbAddress);

    public long getNativeRowBatch() {
        return nativeRowBatch;
    }

    public Row[] getRows() {
        return rows;
    }

    public int getRowCount() {
        return rowCount;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            freeRowBatchNative(getNativeRowBatch());
        } else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_DOUBLE_FREE, "row batch has been closed:" + this
                    + ",threadName:" + Thread.currentThread().getName() + ",native:" + nativeRowBatch);
        }
    }
}