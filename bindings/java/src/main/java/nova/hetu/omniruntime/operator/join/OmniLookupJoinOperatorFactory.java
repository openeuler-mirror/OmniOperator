package nova.hetu.omniruntime.operator.join;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

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
                toNativeConstants(context.probeTypes),
                context.probeOutputCols,
                context.probeHashCols,
                context.buildOutputCols,
                toNativeConstants(context.buildOutputTypes),
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
            Context that = (Context) o;
            return hashBuilderOperatorFactory == that.hashBuilderOperatorFactory
                    && Objects.equals(probeTypes, that.probeTypes)
                    && Objects.equals(probeOutputCols, that.probeOutputCols)
                    && Objects.equals(probeHashCols, that.probeHashCols)
                    && Objects.equals(buildOutputCols, that.buildOutputCols)
                    && Objects.equals(buildOutputTypes, that.buildOutputTypes);
        }
    }
}
