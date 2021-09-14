/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.ContainerVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

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
        super(allocator, vectorCount * BYTES, vectorCount, ContainerVecType.CONTAINER);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    public ContainerVec(int vectorCount, int positionCount, long[] vectorAddresses, VecType[] vecTypes) {
        super(vectorCount * BYTES, vectorCount, ContainerVecType.CONTAINER);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
        put(vectorAddresses, 0, 0, vectorAddresses.length);
    }

    /**
     * this constructor is for JNI to initiate. The 'positionCount' is the number of row of all vectors in this ContainerVec.
     * The number of element in this ContainerVec is the 'size' attribute of Vec.
     *
     * @param nativeVector
     */
    public ContainerVec(long nativeVector) {
        super(nativeVector, ContainerVecType.CONTAINER);
        // get other attributes from native
        this.positionCount = getPositionNative(nativeVector);
        this.vecTypes = VecTypeSerializer.deserialize(getVecTypesNative(nativeVector));
    }

    /**
     * For slicing
     *
     * @param containerVec
     * @param start
     * @param end
     */
    private ContainerVec(ContainerVec containerVec, int start, int end) {
        super(containerVec, start, end, true);
    }

    private ContainerVec(ContainerVec vector, int[] positions, int offset, int length, int positionCount,
            VecType[] vecTypes) {
        super(vector, positions, offset, length);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
    }

    private ContainerVec(ContainerVec vector, int offset, int length, boolean isSlice, int positionCount,
            VecType[] vecTypes) {
        super(vector, offset, length, isSlice);
        this.positionCount = positionCount;
        this.vecTypes = vecTypes;
    }

    /**
     * This constructor of vector is just for shuffle compilation to pass, it will be removed later
     *
     * @param data            data of vector
     * @param capacityInBytes size in bytes of data
     */
    @Deprecated
    public ContainerVec(ByteBuffer data, int capacityInBytes) {
        super(capacityInBytes, data.limit(), ContainerVecType.CONTAINER);
    }

    private static native int getPositionNative(long nativeVector);

    private static native String getVecTypesNative(long nativeVector);

    public long get(int index) {
        return getValues().getLong((index + getOffset()) * BYTES);
    }

    public void set(int index, long value) {
        getValues().putLong((index + getOffset()) * BYTES, value);
    }

    public void put(long[] values, int offset, int start, int length) {
        LongBuffer buffer = getValues().asLongBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
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

    @Override
    public ContainerVec slice(int start, int end) {
        return new ContainerVec(this, start + getOffset(), end - start);
    }

    @Override
    public ContainerVec copy() {
        return null;
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        return new ContainerVec(this, positions, offset, length, positionCount, vecTypes);
    }

    @Override
    public Vec copyRegion(int positionOffset, int length) {
        return new ContainerVec(this, positionOffset, length, false, positionCount, vecTypes);
    }
}
