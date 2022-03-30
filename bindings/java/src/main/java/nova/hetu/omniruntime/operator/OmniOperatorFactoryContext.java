/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * The type Omni operator factory context.
 *
 * @param <T> the type parameter
 * @since 2021-06-30
 */
public abstract class OmniOperatorFactoryContext<T extends OmniJitContext> {
    private static final Cache<OmniJitContext, Long> JIT_CONTEXT_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(java.time.Duration.ofHours(24)).maximumSize(100000).build();

    /**
     * Whether the omni operator factory needs to be cached.
     */
    private boolean isNeedCache = true;

    private final long nativeJitContext;

    private final OmniJitContext jitContext;

    /**
     * Instantiates a new Omni operator factory context.
     *
     * @param jitContext the jit context
     */
    public OmniOperatorFactoryContext(T jitContext) {
        try {
            this.nativeJitContext = JIT_CONTEXT_CACHE.get(jitContext, () -> createNativeJitContext(jitContext));
        } catch (ExecutionException e) {
            throw new RuntimeException("Get instance failed.");
        }
        this.jitContext = jitContext;
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

    /**
     * Gets native jit context.
     *
     * @return the native jit context
     */
    public long getNativeJitContext() {
        return nativeJitContext;
    }

    /**
     * Gets jit context.
     *
     * @return the jit context
     */
    public T getJitContext() {
        return (T) jitContext;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jitContext);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OmniOperatorFactoryContext context = (OmniOperatorFactoryContext) obj;
        return jitContext.equals(context.jitContext);
    }

    /**
     * Create native jit context long.
     *
     * @param context the context
     * @return the long
     */
    protected abstract long createNativeJitContext(T context);
}
