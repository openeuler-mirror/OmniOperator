package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.BitMapHelper;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_BOOLEAN;

public class BooleanVec
        extends FixedWidthVec
{
    private static final int BYTES = 0;
    private static final int FALSE = 0;
    private static final int TRUE = 1;

    public BooleanVec(int size)
    {
        super(BitMapHelper.computeSizeInBytes(size), size, OMNI_VEC_TYPE_BOOLEAN);
    }

    public BooleanVec(VecAllocator allocator, int size)
    {
        super(allocator, BitMapHelper.computeSizeInBytes(size), size, OMNI_VEC_TYPE_BOOLEAN);
    }

    protected BooleanVec(long nativeVector)
    {
        super(nativeVector);
    }

    private BooleanVec(BooleanVec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    /**
     * in the specified position, set the corresponding value
     *
     * @param index position of element
     * @param value 0 is false, others are true
     */
    public void set(int index, int value)
    {
        int newIndex = index + getOffset();
        getValueNulls().set(newIndex);
        if (value != FALSE) {
            BitMapHelper.set(getValues(), newIndex);
        }
        else {
            BitMapHelper.unset(getValues(), newIndex);
        }
    }

    public boolean get(int index)
    {
        int newIndex = index + getOffset();
        if (isNull(newIndex)) {
            return false;
        }
        return BitMapHelper.get(getValues(), newIndex) == TRUE;
    }

    @Override
    public BooleanVec slice(int startIdx, int endIdx)
    {
        return new BooleanVec(this, startIdx, endIdx - startIdx);
    }

    @Override
    public Vec copy()
    {
        return null;
    }
}
