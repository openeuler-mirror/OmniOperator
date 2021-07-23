/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

/**
 * The type Omni operator factory context.
 *
 * @since 20210630
 */
public abstract class OmniOperatorFactoryContext {
    @Override
    public int hashCode() {
        throw new RuntimeException("Unsupported hashCode");
    }

    @Override
    public boolean equals(Object that) {
        throw new RuntimeException("Unsupported equals");
    }
}
