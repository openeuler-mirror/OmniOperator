package nova.hetu.omniruntime.operator.join;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;

import java.util.Objects;

public class OmniLookupJoinOperatorFactory
        extends OmniOperatorFactory
{
    private final int[] probeTypes;
    private final int[] probeOutputCols;
    private final int[] probeHashCols;
    private final int[] buildOutputCols;
    private final int[] buildOutputTypes;
    private final long hashBuilderOperatorFactory;

    public OmniLookupJoinOperatorFactory(int[] probeTypes,
            int[] probeOutputCols,
            int[] probeHashCols,
            int[] buildOutputCols,
            int[] buildOutputTypes,
            OmniHashBuilderOperatorFactory hashBuilderOperatorFactory)
    {
        this.probeTypes = probeTypes;
        this.probeOutputCols = probeOutputCols;
        this.probeHashCols = probeHashCols;
        this.buildOutputCols = buildOutputCols;
        this.buildOutputTypes = buildOutputTypes;
        this.hashBuilderOperatorFactory = hashBuilderOperatorFactory.getNativeOperatorFactory();
    }

    @Override
    protected long createNativeOperatorFactory()
    {
        return createLookupJoinOperatorFactory(probeTypes,
                probeOutputCols,
                probeHashCols,
                buildOutputCols,
                buildOutputTypes,
                hashBuilderOperatorFactory);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, hashBuilderOperatorFactory);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OmniLookupJoinOperatorFactory that = (OmniLookupJoinOperatorFactory) o;
        return hashBuilderOperatorFactory == that.hashBuilderOperatorFactory
                && Objects.equals(probeTypes, that.probeTypes)
                && Objects.equals(probeOutputCols, that.probeOutputCols)
                && Objects.equals(probeHashCols, that.probeHashCols)
                && Objects.equals(buildOutputCols, that.buildOutputCols)
                && Objects.equals(buildOutputTypes, that.buildOutputTypes);
    }

    private static native long createLookupJoinOperatorFactory(int[] probeTypes, int[] probeOutputCols, int[] probeHashCols, int[] buildOutputCols, int[] buildOutputTypes, long hashBuilderOperatorFactory);
}
