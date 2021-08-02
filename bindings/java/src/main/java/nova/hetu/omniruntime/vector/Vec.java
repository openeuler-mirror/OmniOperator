/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.OmniLibs;
import nova.hetu.omniruntime.constants.VecType;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_CONTAINER;
import static nova.hetu.omniruntime.vector.VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

/**
 * base class of vec
 *
 * @since 2021-07-17
 */
@NotThreadSafe
public abstract class Vec
        implements Closeable {
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
    protected int capacityInBytes;

    /**
     * The actual number of value.
     */
    protected int size;

    /**
     * When a vector has been sliced,
     * this value will point to where is the new slice {@link Vec} start.
     */
    protected final int offset;

    /**
     * The value buffer.
     */
    protected final ByteBuffer values;

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

    private Vec(VecAllocator allocator, long nativeVector, int capacityInBytes, int size, int offset, VecType type, boolean isWritable) {
        this.allocator = allocator;
        this.capacityInBytes = capacityInBytes;
        this.size = size;
        this.type = type;
        this.offset = offset;
        this.nativeVector = nativeVector;
        this.values = getValuesNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN);
        this.valueNulls = new ValueNulls(getValueNullsNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN));
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
    public Vec(VecAllocator allocator, int capacityInBytes, int size, VecType type) {
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
    public Vec(int capacityInBytes, int size, VecType type) {
        this(GLOBAL_VECTOR_ALLOCATOR, capacityInBytes, size, type);
    }

    /**
     * The routine is just for slicing and copyRegion vector operator.
     *
     * @param vec the vector need to be sliced or copyRegion
     * @param offset When a vector has been sliced or copyRegion, this value will point to where is the new slice {@link Vec} start.
     * @param length the number of value.
     * @param isSlice Whether the current vector is sliced
     */
    protected Vec(Vec vec, int offset, int length, boolean isSlice) {
        this(vec.allocator,
                isSlice ? sliceVectorNative(vec.nativeVector, offset, length) : copyRegionNative(vec.nativeVector, offset, length),
                vec.getCapacityInBytes(),
                length,
                isSlice ? offset + vec.getOffset() : 0,
                vec.getType(),
                !isSlice);
        if (!isSlice) {
            capacityInBytes = getValues().capacity();
        }
    }

    /**
     * The routine is just for copyPosition vector operator.
     *
     * @param vec the vector need to be copy.
     * @param positions the original vector positions
     * @param offset offset of positions in the input parameter
     * @param length number of elements copied
     */
    protected Vec(Vec vec, int[] positions, int offset, int length) {
        this(vec.allocator,
                copyPositionsNative(vec.nativeVector, positions, offset, length),
                0,
                length,
                0,
                vec.getType(),
                true);
        capacityInBytes = getValues().capacity();
    }

    protected Vec(long nativeVector) {
        this.allocator = new VecAllocator(getAllocatorNative(nativeVector));
        this.capacityInBytes = getCapacityInBytesNative(nativeVector);
        this.size = getSizeNative(nativeVector);
        this.type = new VecType(getTypeNative(nativeVector));
        this.offset = getOffsetNative(nativeVector);
        this.nativeVector = nativeVector;
        this.values = getValuesNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN);
        if (OMNI_VEC_TYPE_CONTAINER.equals(this.type)) {
            System.out.println("NativeVector addr: " + nativeVector + ". In Vec constructor double vec addr : " + this.values.getLong(0));
        }
        this.valueNulls = new ValueNulls(getValueNullsNative(nativeVector).order(ByteOrder.LITTLE_ENDIAN));
    }

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
        setValueCountNative(nativeVector, size);
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
        return values;
    }

    /**
     * get value nulls buffer
     *
     * @return nulls value buffer
     */
    public ValueNulls getValueNulls() {
        return valueNulls;
    }

    /**
     * specify whether the position element is null
     *
     * @param index the element offset in vec
     * @return if it is null, return true, otherwise return false
     */
    public boolean isNull(int index) {
        return valueNulls.get(index + offset);
    }

    /**
     * set the element at the specified position to a null value
     *
     * @param index the element offset in vec
     */
    public void setNull(int index) {
        valueNulls.set(index + offset);
    }

    public void setNulls(int index, boolean[] isNulls, int start, int length) {
        valueNulls.set(index, isNulls, start, length);
    }

    public boolean hasNullValue() {
        boolean[] currentValueNulls = new boolean[size];
        valueNulls.get(offset, currentValueNulls, 0, size);
        boolean hasNullValue = false;
        int start = 0;
        int end = currentValueNulls.length - 1;
        while (start <= end) {
            if (currentValueNulls[start] || currentValueNulls[end]) {
                hasNullValue = true;
                break;
            }
            start++;
            end--;
        }
        return hasNullValue;
    }

    /**
     * return null value array from 0 to size + offset length
     * @return raw value nulls
     */
    public boolean[] getRawValueNulls() {
        // the length of the array is size + offset, so that the caller
        // and vec can have the same offset.
        boolean[] rawValueNulls = new boolean[size + offset];
        valueNulls.get(0, rawValueNulls, 0, rawValueNulls.length);
        return rawValueNulls;
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
     * @param start starting index
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
     * @param offset position offset
     * @param length the number of elements to be copied
     * @return new vec
     */
    public abstract Vec copyPositions(int[] positions, int offset, int length);

    /**
     * copy a vec based on the starting position and the number of elements
     *
     * @param positionOffset staring position
     * @param length the number of elements
     * @return new vec
     */
    public abstract Vec copyRegion(int positionOffset, int length);

    /**
     * This method takes input a source vector to append to the destination vector only If
     * the destination vector has enough available positions.
     *
     * @param other Source Vector to be appended
     * @param offset Number of Positions already occupied
     * @param length Number of Positions in the Source Vector
     */
    public void append(Vec other, int offset, int length) {
        appendVectorNative(this.nativeVector, offset, other.nativeVector, length);
    }

    @Override
    public void close() {
        if (isCloseable) {
            freeVectorNative(this.allocator.getNativeAllocator(), this.nativeVector);
        }
    }

    /**
     * set whether vec can be closed
     *
     * @param isCloseable can vec be closed
     */
    public void setClosable(boolean isCloseable) {
        this.isCloseable = isCloseable;
    }

    /**
     * |type|size|offset|isNullable|isVariable|data|nulls|valueOffsets|
     **/
    private static native long newVectorNative(int capacityInBytes, int size, int type, long allocator);

    private static native long sliceVectorNative(long nativeVector, int offset, int length);

    private static native long copyPositionsNative(long nativeVector, int[] positions, int offset, int length);

    private static native long copyRegionNative(long nativeVector, int positionOffset, int length);

    private static native void freeVectorNative(long allocator, long nativeVector);

    private static native long getAllocatorNative(long nativeVector);

    private static native int getCapacityInBytesNative(long nativeVector);

    private static native int getSizeNative(long nativeVector);

    private static native int getOffsetNative(long nativeVector);

    private static native int setValueCountNative(long nativeVector, int valueCount);

    /**
     * get type from native vector
     *
     * @param nativeVector native vector address
     * @return vec type
     */
    protected static native int getTypeNative(long nativeVector);

    private static native ByteBuffer getValuesNative(long nativeVector);

    private static native ByteBuffer getValueNullsNative(long nativeVector);

    /**
     * merge two vectors
     *
     * @param destNativeVector target native vector
     * @param positionOffset position offset
     * @param srcNativeVector source native vector
     * @param length the number of element
     */
    protected static native void appendVectorNative(long destNativeVector, int positionOffset, long srcNativeVector, int length);
}
