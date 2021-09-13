/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.OmniLibs;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

import static nova.hetu.omniruntime.vector.VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

/**
 * base class of vec
 *
 * @since 2021-07-17
 */
@NotThreadSafe
public abstract class Vec implements Closeable {
    static {
        OmniLibs.load();
    }

    /**
     * When a vector has been sliced,
     * this value will point to where is the new slice {@link Vec} start.
     */
    protected final int offset;

    /**
     * The value buffer.
     */
    protected final OmniBuf valuesBuf;

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
     * The nulls of vector, it is a bitmap.
     */
    private final OmniBuf nullsBuf;

    /**
     * The capacity in bytes of this vector.
     */
    protected int capacityInBytes;

    /**
     * The actual number of value.
     */
    protected int size;

    /**
     * When a vector has been sliced.
     * The current vector and sliced vector are unwritable.
     */
    private boolean isWritable = true;

    private boolean isCloseable = true;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private Vec(VecAllocator allocator, long nativeVector, int capacityInBytes, int size, int offset, VecType type,
            boolean isWritable) {
        this.allocator = allocator;
        this.capacityInBytes = capacityInBytes;
        this.size = size;
        this.type = type;
        this.offset = offset;
        this.nativeVector = nativeVector;
        this.valuesBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValuesNative(nativeVector), capacityInBytes).order(ByteOrder.LITTLE_ENDIAN));
        this.nullsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueNullsNative(nativeVector), size).order(ByteOrder.LITTLE_ENDIAN));
        this.isWritable = isWritable;
    }

    private Vec(VecAllocator allocator, long nativeVector, int size, int offset, VecType type, boolean isWritable) {
        this(allocator, nativeVector, getCapacityInBytesNative(nativeVector), size, offset, type, isWritable);
    }

    /**
     * The routine will use the specialized vector allocator to allocate
     * new vector.
     *
     * @param capacityInBytes the capacity in bytes of vector.
     * @param size            the actual number of value of vector.
     * @param type            the type of this vector.
     * @param allocator       the specialized vector allocator.
     */
    public Vec(VecAllocator allocator, int capacityInBytes, int size, VecType type) {
        this(allocator, newVectorNative(allocator.getNativeAllocator(), capacityInBytes, size,
                type.getId().ordinal()), capacityInBytes, size, 0, type, true);
    }

    /**
     * Ihe routine will use GLOBAL memory pool
     * when there is no specialized vector allocator.
     *
     * @param capacityInBytes the number of value of vector.
     * @param size            the actual number of value of vector.
     * @param type            the type of this vector.
     */
    public Vec(int capacityInBytes, int size, VecType type) {
        this(GLOBAL_VECTOR_ALLOCATOR, capacityInBytes, size, type);
    }

    /**
     * The routine is just for slicing and copyRegion vector operator.
     *
     * @param vec     the vector need to be sliced or copyRegion
     * @param offset  When a vector has been sliced or copyRegion, this value will point to where is the new slice {@link Vec} start.
     * @param length  the number of value.
     * @param isSlice Whether the current vector is sliced
     */
    protected Vec(Vec vec, int offset, int length, boolean isSlice) {
        this(vec.allocator,
                isSlice ? sliceVectorNative(vec.nativeVector, offset, length) :
                        copyRegionNative(vec.nativeVector, offset, length),
                length,
                isSlice ? offset + vec.offset : 0,
                vec.type,
                !isSlice);
    }

    /**
     * The routine is just for copyPosition vector operator.
     *
     * @param vec       the vector need to be copy.
     * @param positions the original vector positions
     * @param offset    offset of positions in the input parameter
     * @param length    number of elements copied
     */
    protected Vec(Vec vec, int[] positions, int offset, int length) {
        this(vec.allocator, copyPositionsNative(vec.nativeVector, positions, offset, length), length, 0,
                vec.type, true);
    }

    protected Vec(long nativeVector, VecType type) {
        this(new VecAllocator(getAllocatorNative(nativeVector)),
                nativeVector,
                getCapacityInBytesNative(nativeVector),
                getSizeNative(nativeVector),
                getOffsetNative(nativeVector),
                type,
                true);
    }

    protected Vec(Vec dictionary, int[] ids, VecType type) {
        this.nativeVector = newDictionaryVectorNative(dictionary.getNativeVector(), ids);
        this.allocator = dictionary.allocator;
        this.capacityInBytes = getCapacityInBytesNative(nativeVector);
        this.size = ids.length;
        this.type = type;
        this.offset = 0;
        this.valuesBuf = null;
        this.nullsBuf = null;
    }

    private static native long newVectorNative(long allocator, int capacityInBytes, int size, int typeId);

    private static native long newDictionaryVectorNative(long nativeDictionary, int[] ids);

    private static native void freeVectorNative(long allocator, long nativeVector);

    private static native long sliceVectorNative(long nativeVector, int offset, int length);

    private static native long copyPositionsNative(long nativeVector, int[] positions, int offset, int length);

    private static native long copyRegionNative(long nativeVector, int positionOffset, int length);

    private static native long getAllocatorNative(long nativeVector);

    private static native int getCapacityInBytesNative(long nativeVector);

    private static native int getSizeNative(long nativeVector);

    private static native int setSizeNative(long nativeVector, int valueCount);

    private static native int getOffsetNative(long nativeVector);

    /**
     * get type from native vector
     *
     * @param nativeVector native vector address
     * @return vec type
     */
    protected static native String getTypeNative(long nativeVector);

    private static native long getValuesNative(long nativeVector);

    private static native long getValueNullsNative(long nativeVector);

    /**
     * merge two vectors
     *
     * @param destNativeVector target native vector
     * @param positionOffset   position offset
     * @param srcNativeVector  source native vector
     * @param length           the number of element
     */
    protected static native void appendVectorNative(long destNativeVector, int positionOffset, long srcNativeVector,
            int length);

    /**
     * get native vector
     *
     * @return native vector address
     */
    public long getNativeVector() {
        return nativeVector;
    }

    /**
     * the size of vector
     *
     * @return size
     */
    public int getSize() {
        return size;
    }

    /**
     * set size of vector
     *
     * @param size size value
     */
    public void setSize(int size) {
        this.size = size;
        setSizeNative(nativeVector, size);
    }

    /**
     * capacity in bytes of vector
     *
     * @return capacity
     */
    public int getCapacityInBytes() {
        return capacityInBytes;
    }

    /**
     * the offset of element in vec
     *
     * @return offset value
     */
    public int getOffset() {
        return offset;
    }

    /**
     * vector type
     *
     * @return vec type
     */
    public VecType getType() {
        return type;
    }

    /**
     * get values buffer
     *
     * @return values buffer
     */
    public ByteBuffer getValues() {
        return valuesBuf.getBuffer();
    }

    /**
     * get value nulls buffer
     *
     * @return nulls value buffer
     */
    public ByteBuffer getValueNulls() {
        return nullsBuf.getBuffer();
    }

    /**
     * specify whether the position element is null
     *
     * @param index the element offset in vec
     * @return if it is null, return true, otherwise return false
     */
    public boolean isNull(int index) {
        return nullsBuf.getByte(index + offset) == 1;
    }

    /**
     * set the element at the specified position to a null value
     *
     * @param index the element offset in vec
     */
    public void setNull(int index) {
        nullsBuf.setByte(index + offset, (byte) 1);
    }

    /**
     * set nulls in batch
     *
     * @param index the offset of the element
     * @param isNulls array of null values, true is null otherwise non-null.
     * @param start array offset
     * @param length number of elements
     */
    public void setNulls(int index, boolean[] isNulls, int start, int length) {
        byte[] values = transformBooleanToByte(isNulls, start, length);
        nullsBuf.setBytes(index, values, 0, length);
    }

    /**
     * whether there is a null value
     *
     * @return if yes, return true otherwise false
     */
    public boolean hasNullValue() {
        byte[] currentValueNulls = new byte[size];
        nullsBuf.getBytes(offset, currentValueNulls, 0, size);
        boolean hasNullValue = false;
        int start = 0;
        int end = currentValueNulls.length - 1;
        while (start <= end) {
            if (currentValueNulls[start] == 1 || currentValueNulls[end] == 1) {
                hasNullValue = true;
                break;
            }
            start++;
            end--;
        }
        return hasNullValue;
    }

    /**
     * transform boolean array to byte array
     *
     * @param values nulls array
     * @param start array offset
     * @param length number of elements
     * @return byte array
     */
    protected byte[] transformBooleanToByte(boolean[] values, int start, int length) {
        byte[] transformedBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            if (values[i + start]) {
                transformedBytes[i] = (byte) 1;
            } else {
                transformedBytes[i] = (byte) 0;
            }
        }

        return transformedBytes;
    }

    /**
     * transform byte array to boolean array
     *
     * @param values byte array, 1 means null, 0 means non-null
     * @param start array offset
     * @param length number of elements
     * @return boolean array
     */
    protected boolean[] transformByteToBoolean(byte[] values, int start, int length) {
        boolean[] transformedBoolean = new boolean[length];
        for (int i = 0; i < length; i++) {
            transformedBoolean[i] = values[i + start] == 1;
        }
        return transformedBoolean;
    }

    /**
     * return null value array from 0 to size + offset length
     *
     * @return raw value nulls
     *
     * @return raw nulls array
     */
    public boolean[] getRawValueNulls() {
        // the length of the array is size + offset, so that the caller
        // and vec can have the same offset.
        byte[] rawValueNulls = new byte[size + offset];
        nullsBuf.getBytes(0, rawValueNulls, 0, rawValueNulls.length);
        return transformByteToBoolean(rawValueNulls, 0, rawValueNulls.length);
    }

    /**
     * get the specified nulls array at the specified absolute
     *
     * @param index the offset of element in vec
     * @param length the number of element
     * @return boolean array
     */
    public boolean[] getValuesNulls(int index, int length) {
        byte[] nullsArray = new byte[length];
        nullsBuf.getBytes(index, nullsArray, 0, length);
        return transformByteToBoolean(nullsArray, 0, length);
    }

    /**
     * is vec writable
     *
     * @return if it is writable, return true, otherwise return false
     */
    public boolean isWritable() {
        return isWritable;
    }

    /**
     * split a vec into two vec according to the specified index and length
     *
     * @param start  starting index
     * @param length number of elements
     * @return new vec
     */
    public abstract Vec slice(int start, int length);

    /**
     * copy a new vec according to the vec
     *
     * @return new vec
     */
    public abstract Vec copy();

    /**
     * copy a new vec based on the positions
     *
     * @param positions all positions in vec
     * @param offset    position offset
     * @param length    the number of elements to be copied
     * @return new vec
     */
    public abstract Vec copyPositions(int[] positions, int offset, int length);

    /**
     * copy a vec based on the starting position and the number of elements
     *
     * @param start  staring position
     * @param length the number of elements
     * @return new vec
     */
    public abstract Vec copyRegion(int start, int length);

    /**
     * This method takes input a source vector to append to the destination vector only If
     * the destination vector has enough available positions.
     *
     * @param other  Source Vector to be appended
     * @param offset Number of Positions already occupied
     * @param length Number of Positions in the Source Vector
     */
    public void append(Vec other, int offset, int length) {
        appendVectorNative(this.nativeVector, offset, other.nativeVector, length);
    }

    @Override
    public void close() {
        if (!isCloseable) {
            return;
        }
        if (isClosed.compareAndSet(false, true)) {
            freeVectorNative(this.allocator.getNativeAllocator(), this.nativeVector);
        } else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_DOUBLE_FREE, "vec has been closed:" + this +
                    ",threadName:" + Thread.currentThread().getName() + ",native:" + nativeVector);
        }
    }

    /**
     * vec is closed
     *
     * @return true is closed, otherwise it it not closed.
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * set whether vec can be closed
     *
     * @param isCloseable can vec be closed
     */
    public void setClosable(boolean isCloseable) {
        this.isCloseable = isCloseable;
    }
}
