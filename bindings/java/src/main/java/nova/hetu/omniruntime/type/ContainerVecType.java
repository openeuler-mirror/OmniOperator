/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * container vec type
 *
 * @since 2021-07-17
 */
public class ContainerVecType extends VecType {
    /**
     * Container singleton
     */
    public static final ContainerVecType CONTAINER = new ContainerVecType();

    /**
     * Container construct
     */
    public ContainerVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_CONTAINER);
    }
}
