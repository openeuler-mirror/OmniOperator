/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.ContainerDataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import java.nio.ByteBuffer;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_VEC_ENCODING_CONTAINER;

/**
 * container vec
 *
 * @since 2021-08-05
 */
public class ContainerVec extends FixedWidthVec {
    private static final int BYTES = Long.BYTES;

    private int positionCount;

    private DataType[] dataTypes;

    public ContainerVec(VecAllocator allocator, int vectorCount, int positionCount, long[] vectorAddresses,
                        DataType[] dataTypes) {
        super(allocator, vectorCount * BYTES, positionCount, OMNI_VEC_ENCODING_CONTAINER,
                ContainerDataType.CONTAINER);
        this.positionCount = positionCount;
        this.dataTypes = dataTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    public ContainerVec(int vectorCount, int positionCount, long[] vectorAddresses, DataType[] dataTypes) {
        super(vectorCount * BYTES, positionCount, OMNI_VEC_ENCODING_CONTAINER, ContainerDataType.CONTAINER);
        this.positionCount = positionCount;
        this.dataTypes = dataTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    /**
     * this constructor is for JNI to initiate. The 'positionCount' is the number of
     * row of all vectors in this ContainerVec. The number of element in this
     * ContainerVec is the 'size' attribute of Vec.
     *
     * @param nativeVector native vector address
     */
    public ContainerVec(long nativeVector) {
        super(nativeVector, ContainerDataType.CONTAINER);
        // get other attributes from native
        this.positionCount = getPositionNative(nativeVector);
        this.dataTypes = DataTypeSerializer.deserialize(getDataTypesNative(nativeVector));
    }

    public ContainerVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, ContainerDataType.CONTAINER);
        // get other attributes from native
        this.positionCount = getPositionNative(nativeVector);
        this.dataTypes = DataTypeSerializer.deserialize(getDataTypesNative(nativeVector));
    }

    private ContainerVec(ContainerVec containerVec, int start, int length, boolean isSlice, DataType[] dataTypes) {
        super(containerVec, start, length, isSlice);
        this.positionCount = length;
        this.dataTypes = dataTypes;
        // for container vec offset is always 0.
        offset = 0;
    }

    private ContainerVec(ContainerVec vector, int[] positions, int offset, int length, DataType[] dataTypes) {
        super(vector, positions, offset, length);
        this.positionCount = length;
        this.dataTypes = dataTypes;
    }

    /**
     * This constructor of vector is just for shuffle compilation to pass, it will
     * be removed later
     *
     * @param data data of vector
     * @param capacityInBytes size in bytes of data
     */
    @Deprecated
    public ContainerVec(ByteBuffer data, int capacityInBytes) {
        super(capacityInBytes, data.limit(), OMNI_VEC_ENCODING_CONTAINER, ContainerDataType.CONTAINER);
    }

    private static native int getPositionNative(long nativeVector);

    private static native String getDataTypesNative(long nativeVector);

    public long get(int index) {
        return valuesBuf.getLong((index + getOffset()) * BYTES);
    }

    public void set(int index, long value) {
        valuesBuf.setLong((index + getOffset()) * BYTES, value);
    }

    public void put(long[] values, int offset, int start, int length) {
        valuesBuf.setLongArray(offset, values, start, length * BYTES);
    }

    public int getPositionCount() {
        return this.positionCount;
    }

    public DataType[] getDataTypes() {
        return this.dataTypes;
    }

    public long getVector(int index) {
        return get(index);
    }

    /**
     * @return positionCount
     */
    public int getSize() {
        return positionCount;
    }

    @Override
    public ContainerVec slice(int start, int end) {
        return new ContainerVec(this, start, end - start, true, dataTypes);
    }

    @Override
    public ContainerVec copy() {
        return null;
    }

    @Override
    public ContainerVec copyPositions(int[] positions, int offset, int length) {
        return new ContainerVec(this, positions, offset, length, dataTypes);
    }

    @Override
    public ContainerVec copyRegion(int positionOffset, int length) {
        return new ContainerVec(this, positionOffset, length, false, dataTypes);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return getCapacityInBytes();
    }

    @Override
    public VecEncoding getEncoding() {
        return OMNI_VEC_ENCODING_CONTAINER;
    }


    /**
     * get the encoding of vector at index position
     *
     * @param index vector index
     *
     * @return encoding of vector at index position
     * */
    public VecEncoding getVecEncoding(int index) {
        return VecEncoding.values()[getVecEncodingNative(getVector(index))];
    }
}
