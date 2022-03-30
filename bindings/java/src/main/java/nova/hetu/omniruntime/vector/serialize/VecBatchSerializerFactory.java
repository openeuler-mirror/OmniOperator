/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector.serialize;

/**
 * create different types of serialization implementation.
 *
 * @since 2021-09-13
 */
public class VecBatchSerializerFactory {
    /**
     * new a vec batch serializer object.
     *
     * @return protobuf vec batch serializer
     */
    public static VecBatchSerializer create() {
        return new ProtoVecBatchSerializer();
    }
}
