package nova.hetu.omniruntime.operator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import nova.hetu.omniruntime.OmniLibs;

import java.util.concurrent.ExecutionException;

public abstract class OmniOperatorFactory<T extends OmniOperatorFactoryContext>
{
    private static final Cache<OmniOperatorFactoryContext, Long> factoryCache = CacheBuilder.newBuilder()
            .expireAfterAccess(java.time.Duration.ofHours(24))
            .maximumSize(100000)
            .build();

    private long nativeOperatorFactory;

    static {
        OmniLibs.load();
    }

    public long getNativeOperatorFactory()
    {
        return nativeOperatorFactory;
    }

    public OmniOperatorFactory(OmniOperatorFactoryContext context)
    {
        try {
            nativeOperatorFactory = factoryCache.get(context, () -> createNativeOperatorFactory((T) context));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Get instance failed.");
        }
    }

    public OmniOperator createOperator()
    {
        long nativeOperator = createOperatorNative(nativeOperatorFactory);
        return new OmniOperator(nativeOperator);
    }

    protected abstract long createNativeOperatorFactory(T context);

    // createOperator
    private static native long createOperatorNative(long factoryAddress);
}
