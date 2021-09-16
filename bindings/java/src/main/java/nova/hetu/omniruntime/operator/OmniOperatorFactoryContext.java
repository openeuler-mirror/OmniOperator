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
    /**
     * Whether the omni operator factory needs to be cached.
     */
    private boolean needCache = true;

    /**
     * Get the flag needCache whether the omni operator factory needs to be cached.
     *
     * @return the flag needCache
     */
    public boolean isNeedCache() {
        return needCache;
    }

    /**
     * Set the flag needCache whether the omni operator factory needs to be cached.
     *
     * @param needCache the flag needCache
     */
    public void setNeedCache(boolean needCache) {
        this.needCache = needCache;
    }

    @Override
    public int hashCode() {
        throw new RuntimeException("Unsupported hashCode");
    }

    @Override
    public boolean equals(Object that) {
        throw new RuntimeException("Unsupported equals");
    }
}
