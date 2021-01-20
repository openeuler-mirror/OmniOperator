package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * wrapper of the off-heap values to be used by blocks, this is also the place to implement vectorized operations.
 * each subclass implements its own vectorized operation as appropriate
 *
 * The design purpose is to enable:
 * 1. SIMD
 * 2. method fusion (e.g. function invocation is merged into 1 single function
 *
 * Each supported data type will subclass this class to create the type specific operations
 */
public abstract class Vec<T>
{
    protected ByteBuffer data;
    protected OMVectorBase base = new OMVectorBase();

    public Vec(int raw_size) {
        data = OMVectorBase.allocate(raw_size).order(ByteOrder.LITTLE_ENDIAN);
    }

    protected int size = 0;

    public abstract void set(int idx, T value);

    public abstract T get(int idx);

    /**
     * returns the hash of all elements in the vec
     * This is an example of in-situ operations that can be implemented enabling SIMD
     * @return
     *
     */
    public abstract Vec hash();

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec mul(T other);

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec mmul(Vec<T> other);

    /**
     * Another potential SIMD in-situ operation
     * @return
     */
    public abstract Vec filter();

    /**
     * Another potential SIMD in-situ operation
     */
    public abstract Vec groupby(/** how to pass in group by parameters? the columns to be used for group by */);

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec join(Vec other /** how to pass in the join conditions? might require many other columns*/);

    public int size()
    {
        return size;
    }
}
