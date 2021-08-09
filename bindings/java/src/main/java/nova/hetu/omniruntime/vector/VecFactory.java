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
                vector = new VarcharVec(nativeVector);
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                vector = new Decimal128Vec(nativeVector, vecType);
                break;
            case OMNI_VEC_TYPE_DECIMAL256:
                vector = new Decimal256Vec(nativeVector, vecType);
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
}
