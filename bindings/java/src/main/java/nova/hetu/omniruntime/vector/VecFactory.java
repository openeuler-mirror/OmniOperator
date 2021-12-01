/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class VecFactory {
    /**
     * Create vector by native vector address and vector type.
     *
     * @param nativeVector native vector address.
     * @param vecType      vector type.
     * @return a new {@link Vec} object instance.
     */
    public static Vec create(long nativeVector, VecType vecType) {
        Vec vector;
        switch (vecType.getId()) {
            case OMNI_VEC_TYPE_INT:
                vector = new IntVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                vector = new LongVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                vector = new DoubleVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_SHORT:
                vector = new ShortVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                vector = new BooleanVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                vector = new VarcharVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                vector = new Decimal128Vec(nativeVector, vecType);
                break;
            case OMNI_VEC_TYPE_DICTIONARY:
                vector = new DictionaryVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_CONTAINER:
                vector = new ContainerVec(nativeVector);
                break;
            default:
                throw new IllegalArgumentException("Not Support Vec Type " + vecType.getId());
        }
        return vector;
    }

    /**
     * Create vector by native vector address and vector type.
     *
     * @param nativeVector                 native vector address
     * @param nativeVectorValueBufAddress  native vector value buffer address
     * @param nativeVectorNullBufAddress   native vector nulls buffer address
     * @param nativeVectorOffsetBufAddress native vector offsets buffer address
     * @param nativeVectorAllocator        native vector allocator address
     * @param capacityInBytes              capacity in bytes of vector
     * @param size                         size of vector
     * @param offset                       position offset of vector
     * @param vecType                      vector type
     * @return Instance of {@link Vec}
     */
    public static Vec create(long nativeVector, long nativeVectorValueBufAddress, long nativeVectorNullBufAddress,
                             long nativeVectorOffsetBufAddress, long nativeVectorAllocator, int capacityInBytes,
                             int size, int offset, VecType vecType) {
        Vec vector;
        switch (vecType.getId()) {
            case OMNI_VEC_TYPE_INT:
                vector = new IntVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                vector = new LongVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                vector = new DoubleVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_SHORT:
                vector = new ShortVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                vector = new BooleanVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                vector = new VarcharVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorOffsetBufAddress, nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                vector = new Decimal128Vec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset, vecType);
                break;
            case OMNI_VEC_TYPE_DICTIONARY:
                vector = new DictionaryVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            case OMNI_VEC_TYPE_CONTAINER:
                vector = new ContainerVec(nativeVector, nativeVectorValueBufAddress, nativeVectorNullBufAddress,
                    nativeVectorAllocator, capacityInBytes, size, offset);
                break;
            default:
                throw new IllegalArgumentException("Not Support Vec Type " + vecType.getId());
        }
        return vector;
    }
}
