package nova.hetu.omniruntime.operator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public abstract class JOmniOperatorFactory
{
    private static final JniWrapper jniWrapper = new JniWrapper();
    private final long nativeOperatorFactory;

    private static Cache<Integer, Long> omniFactoryCache = CacheBuilder.newBuilder()
            .expireAfterAccess(java.time.Duration.ofHours(24))
            .maximumSize(100000)
            .build();

    public JOmniOperatorFactory(long nativeOperatorFactory)
    {
        this.nativeOperatorFactory = nativeOperatorFactory;
    }

    protected static JniWrapper getJniWrapper()
    {
        return jniWrapper;
    }

    public long getNativeOperatorFactory()
    {
        return nativeOperatorFactory;
    }

    public abstract JOmniOperator createOmniOperator();

    public static Cache<Integer, Long> getOmniFactoryCache()
    {
        return omniFactoryCache;
    }
}
