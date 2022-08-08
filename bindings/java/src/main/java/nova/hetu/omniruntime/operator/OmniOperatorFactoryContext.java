/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

/**
 * The type Omni operator factory context.
 *
 * @since 2021-06-30
 */
public abstract class OmniOperatorFactoryContext {
    /**
     * Whether the omni operator factory needs to be cached.
     */
    private boolean isNeedCache = true;

    /**
     * Instantiates a new Omni operator factory context.
     */
    public OmniOperatorFactoryContext() {
    }

    /**
     * Get the flag needCache whether the omni operator factory needs to be cached.
     *
     * @return the flag needCache
     */
    public boolean isNeedCache() {
        return isNeedCache;
    }

    /**
     * Set the flag needCache whether the omni operator factory needs to be cached.
     *
     * @param isNeedCache the flag needCache
     */
    public void setNeedCache(boolean isNeedCache) {
        this.isNeedCache = isNeedCache;
    }
}
