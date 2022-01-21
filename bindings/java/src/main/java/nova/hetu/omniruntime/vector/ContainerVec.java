/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.ContainerVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.nio.ByteBuffer;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class ContainerVec extends FixedWidthVec {
    private static final int BYTES = Long.BYTES;

    private int positionCount;

    private VecType[] vecTypes;

    public ContainerVec(VecAllocator allocator, int vectorCount, int positionCount, long[] vectorAddresses,
            VecType[] vecTypes) {
        super(allocator, vectorCount * BYTES, positionCount, ContainerVecType.CONTAINER);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    public ContainerVec(int vectorCount, int positionCount, long[] vectorAddresses, VecType[] vecTypes) {
        super(vectorCount * BYTES, positionCount, ContainerVecType.CONTAINER);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    /**
     * this constructor is for JNI to initiate. The 'positionCount' is the number of
     * row of all vectors in this ContainerVec. The number of element in this
     * ContainerVec is the 'size' attribute of Vec.
     *
     * @param nativeVector
     */
    public ContainerVec(long nativeVector) {
        super(nativeVector, ContainerVecType.CONTAINER);
        // get other attributes from native
        this.positionCount = getPositionNative(nativeVector);
        this.vecTypes = VecTypeSerializer.deserialize(getVecTypesNative(nativeVector));
    }

    public ContainerVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, ContainerVecType.CONTAINER);
        // get other attributes from native
        this.positionCount = getPositionNative(nativeVector);
        this.vecTypes = VecTypeSerializer.deserialize(getVecTypesNative(nativeVector));
    }

    private ContainerVec(ContainerVec containerVec, int start, int length, boolean isSlice, VecType[] vecTypes) {
        super(containerVec, start, length, isSlice);
        this.positionCount = length;
        this.vecTypes = vecTypes;
        // for container vec offset is always 0.
        offset = 0;
    }

    private ContainerVec(ContainerVec vector, int[] positions, int offset, int length, VecType[] vecTypes) {
        super(vector, positions, offset, length);
        this.positionCount = length;
        this.vecTypes = vecTypes;
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
        super(capacityInBytes, data.limit(), ContainerVecType.CONTAINER);
    }

    private static native int getPositionNative(long nativeVector);

    private static native String getVecTypesNative(long nativeVector);

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

    public VecType[] getVecTypes() {
        return this.vecTypes;
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
        return new ContainerVec(this, start, end - start, true, vecTypes);
    }

    @Override
    public ContainerVec copy() {
        return null;
    }

    @Override
    public ContainerVec copyPositions(int[] positions, int offset, int length) {
        return new ContainerVec(this, positions, offset, length, vecTypes);
    }

    @Override
    public ContainerVec copyRegion(int positionOffset, int length) {
        return new ContainerVec(this, positionOffset, length, false, vecTypes);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return getCapacityInBytes();
    }
}
