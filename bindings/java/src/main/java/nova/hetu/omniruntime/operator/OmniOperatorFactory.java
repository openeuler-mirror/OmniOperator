/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import nova.hetu.omniruntime.OmniLibs;

import java.util.concurrent.ExecutionException;

/**
 * The type Omni operator factory.
 *
 * @param <T> the type parameter
 * @since 20210630
 */
public abstract class OmniOperatorFactory<T extends OmniOperatorFactoryContext> {
    private static final Cache<OmniOperatorFactoryContext, Long> FACTORY_CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(java.time.Duration.ofHours(24))
        .maximumSize(100000)
        .build();

    private final long nativeOperatorFactory;

    static {
        OmniLibs.load();
    }

    /**
     * Gets native operator factory.
     *
     * @return the native operator factory
     */
    public long getNativeOperatorFactory() {
        return nativeOperatorFactory;
    }

    /**
     * Instantiates a new Omni operator factory.
     *
     * @param context the context
     */
    public OmniOperatorFactory(OmniOperatorFactoryContext context) {
        try {
            nativeOperatorFactory = FACTORY_CACHE.get(context, () -> createNativeOperatorFactory((T) context));
        } catch (ExecutionException e) {
            throw new RuntimeException("Get instance failed.");
        }
    }

    /**
     * Create operator omni operator.
     *
     * @return the omni operator
     */
    public OmniOperator createOperator() {
        long nativeOperator = createOperatorNative(nativeOperatorFactory);
        return new OmniOperator(nativeOperator);
    }

    /**
     * Create native operator factory long.
     *
     * @param context the context
     * @return the long
     */
    protected abstract long createNativeOperatorFactory(T context);

    // createOperator
    private static native long createOperatorNative(long factoryAddress);
}
