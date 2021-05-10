package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.Vec;

import java.util.List;

public abstract class JOmniOperator {
    private final JniWrapper jniWrapper;
    private final long nativeOperator;

    public JOmniOperator(JniWrapper jniWrapper, long nativeOperator)
    {
        this.jniWrapper = jniWrapper;
        this.nativeOperator = nativeOperator;
    }

    protected JniWrapper getJniWrapper()
    {
        return jniWrapper;
    }

    public long getNativeOperator()
    {
        return nativeOperator;
    }

    public abstract int addInput(List<Vec> datas, int[] positionCounts, int pageCount);

    public abstract OMResult[] getOutput();
}
