package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.OmniLibs;
import nova.hetu.omniruntime.constants.VecType;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static nova.hetu.omniruntime.vector.VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

@NotThreadSafe
public abstract class Vec
        implements Closeable
{
    /**
     * The specialized vector allocator.
     */
    private final VecAllocator allocator;

    /**
     * The native vector address.
     */
    private final long nativeVector;

    /**
     * The {@link VecType} of this vector
     */
    private final VecType type;

    /**
     * The capacity in bytes of this vector.
     */
    private final int capacityInBytes;

    /**
     * The actual number of value.
     */
    private int size;

    /**
     * When a vector has been sliced,
     * this value will point to where is the new slice {@link Vec} start.
     */
    private final int offset;

    /**
     * The value buffer.
     */
    private final ByteBuffer values;

    /**
     * The nulls of vector, it is a bitmap.
     */
    private final ValueNulls valueNulls;

    /**
     * When a vector has been sliced.
     * The current vector and sliced vector are unwritable.
     */
    private boolean isWritable = true;

    private boolean isCloseable = true;

    static {
        OmniLibs.load();
    }

    private Vec(VecAllocator allocator, long nativeVector, int capacityInBytes, int size, int offset, VecType type, boolean isWritable)
    {
        this.allocator = allocator;
        this.capacityInBytes = capacityInBytes;
        this.size = size;
        this.type = type;
        this.offset = offset;
        this.nativeVector = nativeVector;
        this.values = getValuesNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN);
        this.valueNulls = new ValueNulls(getValueNullsNative(nativeVector));
        this.isWritable = isWritable;
    }

    /**
     * The routine will use the specialized vector allocator to allocate
     * new vector.
     *
     * @param capacityInBytes the capacity in bytes of vector.
     * @param size the actual number of value of vector.
     * @param type the type of this vector.
     * @param allocator the specialized vector allocator.
     */
    public Vec(VecAllocator allocator, int capacityInBytes, int size, VecType type)
    {
        this(allocator,
                newVectorNative(capacityInBytes, size, type.getValue(), allocator.getNativeAllocator()),
                capacityInBytes,
                size,
                0,
                type,
                true);
    }

    /**
     * Ihe routine will use GLOBAL memory pool
     * when there is no specialized vector allocator.
     *
     * @param capacityInBytes the number of value of vector.
     * @param size the actual number of value of vector.
     * @param type the type of this vector.
     */
    public Vec(int capacityInBytes, int size, VecType type)
    {
        this(GLOBAL_VECTOR_ALLOCATOR, capacityInBytes, size, type);
    }

    /**
     * The routine is just for slicing vector operator.
     *
     * @param vec the vector need to be sliced.
     * @param offset When a vector has been sliced, this value will point to where is the new slice {@link Vec} start.
     * @param length the number of value.
     */
    protected Vec(Vec vec, int offset, int length)
    {
        this(vec.allocator,
                sliceVectorNative(vec.nativeVector, offset, length),
                vec.getCapacityInBytes(),
                length,
                offset,
                vec.getType(),
                false);
    }

    protected Vec(long nativeVector)
    {
        this.allocator = new VecAllocator(getAllocatorNative(nativeVector));
        this.capacityInBytes = getCapacityInBytesNative(nativeVector);
        this.size = getSizeNative(nativeVector);
        this.type = new VecType(getTypeNative(nativeVector));
        this.offset = getOffsetNative(nativeVector);
        this.nativeVector = nativeVector;
        this.values = getValuesNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN);
        this.valueNulls = new ValueNulls(getValueNullsNative(nativeVector));
    }

    public long getNativeVector()
    {
        return nativeVector;
    }

    public int getSize()
    {
        return size;
    }

    public void setSize(int size)
    {
        this.size = size;
        setValueCountNative(nativeVector, size);
    }

    public int getCapacityInBytes()
    {
        return capacityInBytes;
    }

    public int getOffset()
    {
        return offset;
    }

    public VecType getType()
    {
        return type;
    }

    public ByteBuffer getValues()
    {
        return values;
    }

    public ValueNulls getValueNulls()
    {
        return valueNulls;
    }

    public boolean isNull(int index)
    {
        return valueNulls.get(index);
    }

    public void setNull(int index)
    {
        valueNulls.set(index);
    }

    public boolean isWritable()
    {
        return isWritable;
    }

    public abstract Vec slice(int start, int end);

    public abstract Vec copy();

    @Override
    public void close()
    {
        if (isCloseable) {
            freeVectorNative(this.allocator.getNativeAllocator(), this.nativeVector);
        }
    }

    public void setClosable(boolean isCloseable)
    {
        this.isCloseable = isCloseable;
    }

    /**
     * |type|size|offset|isNullable|isVariable|data|nulls|valueOffsets|
     **/
    private static native long newVectorNative(int capacityInBytes, int size, int type, long allocator);

    private static native long sliceVectorNative(long nativeVector, int offset, int length);

    private static native void freeVectorNative(long allocator, long nativeVector);

    private static native long getAllocatorNative(long nativeVector);

    private static native int getCapacityInBytesNative(long nativeVector);

    private static native int getSizeNative(long nativeVector);

    private static native int getOffsetNative(long nativeVector);

    private static native int setValueCountNative(long nativeVector, int valueCount);

    protected static native int getTypeNative(long nativeVector);

    private static native ByteBuffer getValuesNative(long nativeVector);

    private static native ByteBuffer getValueNullsNative(long nativeVector);
}
