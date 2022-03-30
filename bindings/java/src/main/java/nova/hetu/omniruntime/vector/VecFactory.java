/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

/**
 * vec factory.
 *
 * @since 2021-08-05
 */
public class VecFactory {
    /**
     * Create vector by native vector address and data type.
     *
     * @param nativeVector native vector address
     * @param encoding vector encoding
     * @param dataType vector data type
     * @return a new {@link Vec} object instance
     */
    public static Vec create(long nativeVector, VecEncoding encoding, DataType dataType) {
        Vec vector;
        switch (encoding) {
            case OMNI_VEC_ENCODING_FLAT:
                vector = createFlatVec(nativeVector, dataType);
                break;
            case OMNI_VEC_ENCODING_DICTIONARY:
                vector = new DictionaryVec(nativeVector);
                break;
            case OMNI_VEC_ENCODING_CONTAINER:
                vector = new ContainerVec(nativeVector);
                break;
            default:
                throw new IllegalArgumentException("Not Support Vec Encoding " + encoding);
        }
        return vector;
    }

    private static Vec createFlatVec(long nativeVector, DataType dataType) {
        switch (dataType.getId()) {
            case OMNI_INT:
            case OMNI_DATE32:
                return new IntVec(nativeVector);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_DECIMAL64:
                return new LongVec(nativeVector);
            case OMNI_DOUBLE:
                return new DoubleVec(nativeVector);
            case OMNI_SHORT:
                return new ShortVec(nativeVector);
            case OMNI_BOOLEAN:
                return new BooleanVec(nativeVector);
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return new VarcharVec(nativeVector);
            case OMNI_DECIMAL128:
                return new Decimal128Vec(nativeVector, dataType);
            default:
                throw new IllegalArgumentException("Not Support Data Type " + dataType.getId());
        }
    }

    private static Vec createFlatVec(long nativeVector, long nativeVectorValueBufAddress,
            long nativeVectorNullBufAddress, long nativeVectorOffsetBufAddress, long nativeVectorAllocator,
            int capacityInBytes, int size, int offset, DataType dataType) {
        switch (dataType.getId()) {
            case OMNI_INT:
            case OMNI_DATE32:
                return new IntVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_DECIMAL64:
                return new LongVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_DOUBLE:
                return new DoubleVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_SHORT:
                return new ShortVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_BOOLEAN:
                return new BooleanVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return new VarcharVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorOffsetBufAddress, nativeVectorAllocator, capacityInBytes, size, offset);
            case OMNI_DECIMAL128:
                return new Decimal128Vec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset, dataType);
            default:
                throw new IllegalArgumentException("Not Support Data Type " + dataType.getId());
        }
    }

    /**
     * Create vector by native vector address and vector type.
     *
     * @param nativeVector native vector address
     * @param nativeVectorValueBufAddress native vector value buffer address
     * @param nativeVectorNullBufAddress native vector nulls buffer address
     * @param nativeVectorOffsetBufAddress native vector offsets buffer address
     * @param nativeVectorAllocator native vector allocator address
     * @param capacityInBytes capacity in bytes of vector
     * @param size size of vector
     * @param offset position offset of vector
     * @param encoding vector encoding type
     * @param dataType vector data type
     * @return Instance of {@link Vec}
     */
    public static Vec create(long nativeVector, long nativeVectorValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorOffsetBufAddress, long nativeVectorAllocator, int capacityInBytes, int size, int offset,
            VecEncoding encoding, DataType dataType) {
        Vec vector;
        switch (encoding) {
            case OMNI_VEC_ENCODING_FLAT:
                vector = createFlatVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorOffsetBufAddress, nativeVectorAllocator, capacityInBytes, size, offset, dataType);
                break;
            case OMNI_VEC_ENCODING_DICTIONARY:
                vector = new DictionaryVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_ENCODING_CONTAINER:
                vector = new ContainerVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                        nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            default:
                throw new IllegalArgumentException("Not Support Vec Encoding " + encoding);
        }
        return vector;
    }
}
