package nova.hetu.omnicache.vector;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class VarVec<T>
{
    protected int MAX_BUFFER_SIZE = 5*1024*1024;
    protected int[] offsets;
    protected int[] lengths;
    int lastOffsetPosition;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected ByteBuffer[] buffers;
    int used;
    int capacity;

    public VarVec(long capcity, int elements)
    {
        offsets = new int[elements];
        lengths = new int[elements];
        int numberOfBuffers = capacity / MAX_BUFFER_SIZE + 1;
        buffers = new ByteBuffer[numberOfBuffers];
        for (int i=0; i < numberOfBuffers; i++) {
            ByteBuffer buffer = OMVectorBase.allocate(MAX_BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            buffers[i] = buffer;
        }
        lastOffsetPosition = -1;
    }

    public VarVec(ByteBuffer buffer)
    {
        this.buffers = new ByteBuffer[1];
        this.buffers[0] = buffer;
    }


    public void incrRefCount() {
        this.incrRefCount(1);
    }

    public void incrRefCount(int increment) {
        this.referenceCount.addAndGet(increment);
    }

    public void release() {
        this.release(1);
    }

    public void release(int decrement) {
        if (referenceCount.addAndGet(-decrement) == 0) {
            close();
        }
    }

    /**
     * Creates a vector from a slice of the underlying buffer.
     *
     * @param startIdx
     * @param endIdx
     * @return
     */
    public abstract Vec slice(int startIdx, int endIdx);

    /**
     * returns the hash of all elements in the vec
     * This is an example of in-situ operations that can be implemented enabling SIMD
     *
     * @return
     */
    public abstract Vec hash();

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec mul(int other);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec mmul(Vec other);

    /**
     * Another potential SIMD in-situ operation
     *
     * @return
     */
    public abstract Vec filter();

    /**
     * Another potential SIMD in-situ operation
     */
    public abstract Vec groupby(/** how to pass in group by parameters? the columns to be used for group by */);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec join(Vec other /** how to pass in the join conditions? might require many other columns*/);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec concat(Vec other);

    public int size() {
        return offsets.length;
    }

    public int capacity() {
        return capacity;
    }

    public int remaining() {
        return capacity - used;
    }

    public abstract VecType getType();

    public abstract ByteBuffer getData();

    public synchronized void close() {

    }

    // TODO: Handle memory properly when we add OmniCacheManager
    @Override
    protected void finalize() {
        close();
    }
}