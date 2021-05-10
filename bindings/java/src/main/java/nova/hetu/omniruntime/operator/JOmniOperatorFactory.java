package nova.hetu.omniruntime.operator;

public abstract class JOmniOperatorFactory {
    private static final JniWrapper jniWrapper = new JniWrapper();
    private final long nativeOperatorFactory;

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
}
