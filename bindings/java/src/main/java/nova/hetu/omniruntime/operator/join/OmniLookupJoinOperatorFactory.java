package nova.hetu.omniruntime.operator.join;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniLookupJoinOperatorFactory
        extends OmniOperatorFactory<OmniLookupJoinOperatorFactory.Context>
{
    public OmniLookupJoinOperatorFactory(
            VecType[] probeTypes,
            int[] probeOutputCols,
            int[] probeHashCols,
            int[] buildOutputCols,
            VecType[] buildOutputTypes,
            OmniHashBuilderOperatorFactory hashBuilderOperatorFactory)
    {
        super(new Context(probeTypes, probeOutputCols, probeHashCols, buildOutputCols, buildOutputTypes, hashBuilderOperatorFactory));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createLookupJoinOperatorFactory(
                OmniUtils.transformVecType(context.probeTypes),
                context.probeOutputCols,
                context.probeHashCols,
                context.buildOutputCols,
                OmniUtils.transformVecType(context.buildOutputTypes),
                context.hashBuilderOperatorFactory);
    }

    private static native long createLookupJoinOperatorFactory(int[] probeTypes, int[] probeOutputCols, int[] probeHashCols, int[] buildOutputCols, int[] buildOutputTypes, long hashBuilderOperatorFactory);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] probeTypes;
        private final int[] probeOutputCols;
        private final int[] probeHashCols;
        private final int[] buildOutputCols;
        private final VecType[] buildOutputTypes;
        private final long hashBuilderOperatorFactory;

        public Context(VecType[] probeTypes,
                int[] probeOutputCols,
                int[] probeHashCols,
                int[] buildOutputCols,
                VecType[] buildOutputTypes,
                OmniHashBuilderOperatorFactory hashBuilderOperatorFactory)
        {
            this.probeTypes = requireNonNull(probeTypes, "probeTypes");
            this.probeOutputCols = requireNonNull(probeOutputCols, "probeOutputCols");
            this.probeHashCols = requireNonNull(probeHashCols, "probeHashCols");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes");
            requireNonNull(hashBuilderOperatorFactory, "hashBuilderOperatorFactory");
            this.hashBuilderOperatorFactory = hashBuilderOperatorFactory.getNativeOperatorFactory();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(probeTypes), Arrays.hashCode(probeOutputCols), Arrays.hashCode(probeHashCols), Arrays.hashCode(buildOutputCols), Arrays.hashCode(buildOutputTypes), hashBuilderOperatorFactory);
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
            Context that = (Context) o;
            return hashBuilderOperatorFactory == that.hashBuilderOperatorFactory
                    && Arrays.equals(probeTypes, that.probeTypes)
                    && Arrays.equals(probeOutputCols, that.probeOutputCols)
                    && Arrays.equals(probeHashCols, that.probeHashCols)
                    && Arrays.equals(buildOutputCols, that.buildOutputCols)
                    && Arrays.equals(buildOutputTypes, that.buildOutputTypes);
        }
    }
}
