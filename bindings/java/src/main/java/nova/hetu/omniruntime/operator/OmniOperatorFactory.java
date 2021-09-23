/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.vector.VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import nova.hetu.omniruntime.OmniLibs;

import java.util.concurrent.ExecutionException;
import nova.hetu.omniruntime.vector.VecAllocator;

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

    static {
        OmniLibs.load();
    }

    private final long nativeOperatorFactory;

    /**
     * Instantiates a new Omni operator factory.
     *
     * @param context the context
     */
    public OmniOperatorFactory(OmniOperatorFactoryContext context) {
        try {
            if (context.isNeedCache()) {
                nativeOperatorFactory = FACTORY_CACHE.get(context, () -> createNativeOperatorFactory((T) context));
            } else {
                nativeOperatorFactory = createNativeOperatorFactory((T) context);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Get instance failed.");
        }
    }

    // createOperator
    private static native long createOperatorNative(long factoryAddress, long vecAllocatorAddress);

    /**
     * Gets native operator factory.
     *
     * @return the native operator factory
     */
    public long getNativeOperatorFactory() {
        return nativeOperatorFactory;
    }

    /**
     * Create operator omni operator.
     *
     * @return the omni operator
     */
    public OmniOperator createOperator(VecAllocator vecAllocator) {
        if (vecAllocator == null) {
            System.out.println(Thread.getAllStackTraces());
            return createOperator();
        }
        long nativeOperator = createOperatorNative(nativeOperatorFactory, vecAllocator.getNativeAllocator());
        return new OmniOperator(nativeOperator, vecAllocator);
    }

    /**
     * Create operator omni operator.
     *
     * @return the omni operator
     */
    public OmniOperator createOperator() {
        long nativeOperator = createOperatorNative(nativeOperatorFactory, GLOBAL_VECTOR_ALLOCATOR.getNativeAllocator());
        return new OmniOperator(nativeOperator, GLOBAL_VECTOR_ALLOCATOR);
    }

    /**
     * Create native operator factory long.
     *
     * @param context the context
     * @return the long
     */
    protected abstract long createNativeOperatorFactory(T context);
}
