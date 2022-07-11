/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.vector.VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

import com.google.common.annotations.VisibleForTesting;

import nova.hetu.omniruntime.OmniLibs;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * base class of vec.
 *
 * @since 2021-07-17
 */
//@NotThreadSafe
public abstract class Vec implements Closeable {
    /**
     * indicates a null value in a nulls buffer.
     */
    public static final byte NULL = 1;

    /**
     * indicates a not null value in a nulls buffer.
     */
    public static final byte NOT_NULL = 0;

    static {
        OmniLibs.load();
    }

    /**
     * When a vector has been sliced, this value will point to where is the new
     * slice {@link Vec} start.
     */
    protected int offset;

    /**
     * The value buffer.
     */
    protected OmniBuf valuesBuf;

    /**
     * The nulls of vector, it is a bitmap.
     */
    protected final OmniBuf nullsBuf;

    /**
     * The native vector address.
     */
    protected final long nativeVector;

    /**
     * The capacity in bytes of this vector.
     */
    protected int capacityInBytes;

    /**
     * The actual number of value.
     */
    protected int size;

    /**
     * The specialized vector allocator.
     */
    private final VecAllocator allocator;

    /**
     * The {@link DataType} of this vector.
     */
    private DataType dataType;

    /**
     * When a vector has been sliced. The current vector and sliced vector are
     * unwritable.
     */
    private boolean isWritable = true;

    private boolean isCloseable = true;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * The routine will use the specialized vector allocator to allocate new vector.
     *
     * @param allocator the specialized vector allocator
     * @param capacityInBytes the capacity in bytes of vector
     * @param size the actual number of value of vector
     * @param encoding the encoding type of vector
     * @param datatype the data type of this vector
     */
    public Vec(VecAllocator allocator, int capacityInBytes, int size, VecEncoding encoding, DataType datatype) {
        this(allocator, newVectorNative(allocator.getNativeAllocator(), capacityInBytes, size, encoding.ordinal(),
                datatype.getId().toValue()), capacityInBytes, size, 0, datatype, true);
    }

    /**
     * The routine will use GLOBAL memory pool when there is no specialized vector
     * allocator.
     *
     * @param capacityInBytes the number of value of vector
     * @param size the actual number of value of vector
     * @param encoding the encoding type of vector
     * @param dataType the data type of this vector
     */
    public Vec(int capacityInBytes, int size, VecEncoding encoding, DataType dataType) {
        this(GLOBAL_VECTOR_ALLOCATOR, capacityInBytes, size, encoding, dataType);
    }

    /**
     * The routine is just for slicing and copyRegion vector operator.
     *
     * @param vec the vector need to be sliced or copyRegion
     * @param offset When a vector has been sliced or copyRegion, this value will
     *            point to where is the new slice {@link Vec} start
     * @param length the number of value
     * @param isSlice Whether the current vector is sliced
     */
    protected Vec(Vec vec, int offset, int length, boolean isSlice) {
        this(vec.allocator,
                isSlice
                        ? sliceVectorNative(vec.nativeVector, offset, length)
                        : copyRegionNative(vec.nativeVector, offset, length),
                length, isSlice ? offset + vec.offset : 0, vec.dataType, !isSlice);
    }

    /**
     * The routine is just for copyPosition vector operator.
     *
     * @param vec the vector need to be copy
     * @param positions the original vector positions
     * @param offset offset of positions in the input parameter
     * @param length number of elements copied
     */
    protected Vec(Vec vec, int[] positions, int offset, int length) {
        this(vec.allocator, copyPositionsNative(vec.nativeVector, positions, offset, length), length, 0, vec.dataType,
                true);
    }

    /**
     * The routine will use native vector to initialize a new vector.
     *
     * @param nativeVector native vector address
     * @param dataType the type of this vector
     */
    protected Vec(long nativeVector, DataType dataType) {
        this(new VecAllocator(getAllocatorNative(nativeVector)), nativeVector, getCapacityInBytesNative(nativeVector),
                getSizeNative(nativeVector), getOffsetNative(nativeVector), dataType, true);
    }

    /**
     * The routine will use native vector to initialize a new vector.
     *
     * @param nativeVector native vector address
     * @param nativeVectorValueBufAddress valueBuf address of native vector
     * @param nativeVectorNullBufAddress nullBuf address of native vector
     * @param nativeVectorAllocator allocator address of native vector
     * @param capacityInBytes capacity in bytes of vector
     * @param size the actual number of value of vector
     * @param offset offset of positions in the input parameter
     * @param dataType the type of this vector
     */
    protected Vec(long nativeVector, long nativeVectorValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset, DataType dataType) {
        this(new VecAllocator(nativeVectorAllocator), nativeVector, nativeVectorValueBufAddress,
                nativeVectorNullBufAddress, capacityInBytes, size, offset, dataType, true);
    }

    private Vec(VecAllocator allocator, long nativeVector, long nativeVectorValueBufAddress,
            long nativeVectorNullBufAddress, int capacityInBytes, int size, int offset, DataType dataType,
            boolean isWritable) {
        this.allocator = allocator;
        this.capacityInBytes = capacityInBytes;
        this.size = size;
        this.dataType = dataType;
        this.offset = offset;
        this.nativeVector = nativeVector;
        this.valuesBuf = OmniBufFactory.create(nativeVectorValueBufAddress, capacityInBytes);
        this.nullsBuf = OmniBufFactory.create(nativeVectorNullBufAddress, size);
        this.isWritable = isWritable;
    }

    private Vec(VecAllocator allocator, long nativeVector, int capacityInBytes, int size, int offset, DataType dataType,
            boolean isWritable) {
        this(allocator, nativeVector, getValuesNative(nativeVector), getValueNullsNative(nativeVector), capacityInBytes,
                size, offset, dataType, isWritable);
    }

    private Vec(VecAllocator allocator, long nativeVector, int size, int offset, DataType dataType,
            boolean isWritable) {
        this(allocator, nativeVector, getCapacityInBytesNative(nativeVector), size, offset, dataType, isWritable);
    }

    private static native long newVectorNative(long allocator, int capacityInBytes, int size, int vecEncodingId,
            int dataTypeId);

    private static native void freeVectorNative(long allocator, long nativeVector);

    private static native long sliceVectorNative(long nativeVector, int offset, int length);

    private static native long copyPositionsNative(long nativeVector, int[] positions, int offset, int length);

    private static native long copyRegionNative(long nativeVector, int positionOffset, int length);

    private static native long getAllocatorNative(long nativeVector);

    /**
     * get capacity in Bytes from native vector
     *
     * @param nativeVector nativeVector address
     * @return capacity of native vector
     */
    protected static native int getCapacityInBytesNative(long nativeVector);

    private static native int getSizeNative(long nativeVector);

    private static native int setSizeNative(long nativeVector, int valueCount);

    private static native int getOffsetNative(long nativeVector);

    /**
     * get type id from native vector.
     *
     * @param nativeVector native vector address
     * @return vec type
     */
    protected static native int getTypeIdNative(long nativeVector);

    /**
     * get value address from native vector.
     *
     * @param nativeVector native vector address
     * @return value address of native vector
     */
    protected static native long getValuesNative(long nativeVector);

    /**
     * get encoding of native vector.
     *
     * @param nativeVector native vector
     * @return encoding type id
     */
    protected static native int getVecEncodingNative(long nativeVector);

    private static native long getValueNullsNative(long nativeVector);

    /**
     * merge two vectors.
     *
     * @param destNativeVector target native vector
     * @param positionOffset position offset
     * @param srcNativeVector source native vector
     * @param length the number of element
     */
    protected static native void appendVectorNative(long destNativeVector, int positionOffset, long srcNativeVector,
            int length);

    private static native void setNullFlagNative(long nativeVector, boolean newHasNull);

    private static native boolean mayHaveNullNative(long nativeVector);

    private static native void setNullCountNative(long nativeVector, int newNullCount);

    private static native int getNullCountNative(long nativeVector);

    /**
     * get native vector.
     *
     * @return native vector address
     */
    public long getNativeVector() {
        return nativeVector;
    }

    /**
     * the size of vector.
     *
     * @return size
     */
    public int getSize() {
        return size;
    }

    /**
     * set size of vector.
     *
     * @param size size value
     */
    public void setSize(int size) {
        this.size = size;
        setSizeNative(nativeVector, size);
    }

    /**
     * capacity in bytes of vector.
     *
     * @return capacity
     */
    public int getCapacityInBytes() {
        return capacityInBytes;
    }

    /**
     * the offset of element in vec.
     *
     * @return offset value
     */
    public int getOffset() {
        return offset;
    }

    /**
     * vector data type.
     *
     * @return vector data type
     */
    public DataType getType() {
        return dataType;
    }

    /**
     * vector encoding.
     *
     * @return vector encoding
     */
    public VecEncoding getEncoding() {
        return VecEncoding.OMNI_VEC_ENCODING_FLAT;
    }

    /**
     * get values buffer.
     *
     * @return values buffer
     */
    public OmniBuf getValuesBuf() {
        return valuesBuf;
    }

    /**
     * set values buffer.
     *
     * @param buf buf of data
     */
    public void setValuesBuf(byte[] buf) {
        valuesBuf.setBytes(0, buf, 0, buf.length);
    }

    /**
     * get value nulls buffer.
     *
     * @return nulls value buffer
     */
    public OmniBuf getValueNullsBuf() {
        return nullsBuf;
    }

    /**
     * specify whether the position element is null.
     *
     * @param index the element offset in vec
     * @return if it is null, return true, otherwise return false
     */
    public boolean isNull(int index) {
        return nullsBuf.getByte(index + offset) == 1;
    }

    /**
     * set the element at the specified position to a null value.
     *
     * @param index the element offset in vec
     */
    public void setNull(int index) {
        nullsBuf.setByte(index + offset, (byte) 1);
        setNullFlagNative(nativeVector, true);
    }

    /**
     * set nulls in batch.
     *
     * @param index the offset of the element
     * @param isNulls array of null values, true is null otherwise non-null.
     * @param start array offset
     * @param length number of elements
     */
    public void setNulls(int index, boolean[] isNulls, int start, int length) {
        byte[] values = transformBooleanToByte(isNulls, start, length);
        nullsBuf.setBytes(index, values, 0, length);
        setNullFlagNative(nativeVector, true);
    }

    /**
     * set nulls in batch.
     *
     * @param index the offset of the element
     * @param isNulls array of null values, true is null otherwise non-null.
     * @param start array offset
     * @param length number of elements
     */
    public void setNulls(int index, byte[] isNulls, int start, int length) {
        nullsBuf.setBytes(index, isNulls, start, length);
        setNullFlagNative(nativeVector, true);
    }

    /**
     * set nulls buffer.
     *
     * @param buf buf of null
     */
    public void setNullsBuf(byte[] buf) {
        nullsBuf.setBytes(0, buf, 0, buf.length);
    }

    /**
     * whether there is a null value.
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
     * transform boolean array to byte array.
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
     * transform byte array to boolean array.
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
     * return null value array from 0 to size + offset length.
     *
     * @return raw value nulls
     * @return raw nulls array
     */
    public byte[] getRawValueNulls() {
        // the length of the array is size + offset, so that the caller
        // and vec can have the same offset.
        byte[] rawValueNulls = new byte[size + offset];
        nullsBuf.getBytes(0, rawValueNulls, 0, rawValueNulls.length);
        return rawValueNulls;
    }

    /**
     * get the specified nulls array at the specified absolute.
     *
     * @param index the offset of element in vec
     * @param length the number of element
     * @return boolean array
     */
    public boolean[] getValuesNulls(int index, int length) {
        byte[] nullsArray = new byte[length];
        nullsBuf.getBytes(index + offset, nullsArray, 0, length);
        return transformByteToBoolean(nullsArray, 0, length);
    }

    /**
     * is vec writable.
     *
     * @return if it is writable, return true, otherwise return false
     */
    public boolean isWritable() {
        return isWritable;
    }

    /**
     * split a vec into two vec according to the specified index and length.
     *
     * @param start starting index
     * @param length number of elements
     * @return new vec
     */
    public abstract Vec slice(int start, int length);

    /**
     * copy a new vec according to the vec.
     *
     * @return new vec
     */
    public abstract Vec copy();

    /**
     * copy a new vec based on the positions.
     *
     * @param positions all positions in vec
     * @param offset position offset
     * @param length the number of elements to be copied
     * @return new vec
     */
    public abstract Vec copyPositions(int[] positions, int offset, int length);

    /**
     * copy a vec based on the starting position and the number of elements.
     *
     * @param start staring position
     * @param length the number of elements
     * @return new vec
     */
    public abstract Vec copyRegion(int start, int length);

    /**
     * This method takes input a source vector to append to the destination vector
     * only If the destination vector has enough available positions.
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
        if (!isCloseable) {
            return;
        }
        if (isClosed.compareAndSet(false, true)) {
            freeVectorNative(this.allocator.getNativeAllocator(), this.nativeVector);
        } else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_DOUBLE_FREE, "vec has been closed:" + this
                    + ",threadName:" + Thread.currentThread().getName() + ",native:" + nativeVector);
        }
    }

    /**
     * vec is closed.
     *
     * @return true is closed, otherwise it it not closed.
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * set whether vec can be closed.
     *
     * @param isCloseable can vec be closed
     */
    public void setClosable(boolean isCloseable) {
        this.isCloseable = isCloseable;
    }

    /**
     * return the allocator of vector.
     *
     * @return allocator of the vector
     */
    public VecAllocator getAllocator() {
        return allocator;
    }

    /**
     * returns the number of bytes of the data written.
     *
     * @return length in bytes
     */
    public abstract int getRealValueBufCapacityInBytes();

    /**
     * returns the number of bytes of the null buf.
     *
     * @return length in bytes
     */
    public int getRealNullBufCapacityInBytes() {
        return size * Byte.BYTES;
    }

    /**
     * returns the number of bytes of the offsets, for VarcharVec returned according
     * to size calculation, other types of vec return 0.
     *
     * @return length in bytes
     */
    public int getRealOffsetBufCapacityInBytes() {
        return 0;
    }

    @VisibleForTesting
    void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * is it possible the vec may have a null value
     *
     * @return if false, the vec can not contain a null, but if true, the vec may or may not have a null
     */
    public boolean mayHaveNull() {
        return mayHaveNullNative(nativeVector);
    }
    
    /**
     * get the number of nulls in vec
     *
     * @return if 0, the vec can not contain a null
     */
    public int getNullCount() {
        return getNullCountNative(nativeVector);
    }
}
