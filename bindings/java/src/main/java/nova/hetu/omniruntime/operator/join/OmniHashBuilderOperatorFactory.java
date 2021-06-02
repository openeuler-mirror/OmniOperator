package nova.hetu.omniruntime.operator.join;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;

import java.util.Objects;

public class OmniHashBuilderOperatorFactory
        extends OmniOperatorFactory
{
    private final int[] buildTypes;
    private final int[] buildOutputCols;
    private final int[] buildHashCols;
    private final int operatorCount;

    public OmniHashBuilderOperatorFactory(int[] buildTypes,
            int[] buildOutputCols,
            int[] buildHashCols,
            int operatorCount)
    {
        this.buildTypes = buildTypes;
        this.buildOutputCols = buildOutputCols;
        this.buildHashCols = buildHashCols;
        this.operatorCount = operatorCount;
    }

    @Override
    protected long createNativeOperatorFactory()
    {
        return createHashBuilderOperatorFactory(buildTypes, buildOutputCols, buildHashCols, operatorCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(buildTypes, buildOutputCols, buildHashCols);
    }

    private static native long createHashBuilderOperatorFactory(int[] buildTypes, int[] buildOutputCols, int[] buildHashCols, int operatorCount);
}
