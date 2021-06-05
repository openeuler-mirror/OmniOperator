package nova.hetu.omniruntime.operator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import nova.hetu.omniruntime.NativeLibs;

import java.util.concurrent.ExecutionException;

public abstract class OmniOperatorFactory
{
    private static final Cache<Integer, Long> factorCache = CacheBuilder.newBuilder()
            .expireAfterAccess(java.time.Duration.ofHours(24))
            .maximumSize(100000)
            .build();

    private long nativeOperatorFactory;

    static {
        NativeLibs.load();
    }

    public long getNativeOperatorFactory()
    {
        return nativeOperatorFactory;
    }

    public OmniOperator createOperator()
    {
        try {
            nativeOperatorFactory = factorCache.get(hashCode(), () -> createNativeOperatorFactory());
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Get instance failed.");
        }
        long nativeOperator = createOperator(nativeOperatorFactory);
        return new OmniOperator(nativeOperator);
    }

    protected abstract long createNativeOperatorFactory();

    // createOperator
    private static native long createOperator(long factoryAddress);
}
