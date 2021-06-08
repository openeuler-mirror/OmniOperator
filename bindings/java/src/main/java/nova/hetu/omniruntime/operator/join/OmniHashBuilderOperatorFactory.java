package nova.hetu.omniruntime.operator.join;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniHashBuilderOperatorFactory
        extends OmniOperatorFactory<OmniHashBuilderOperatorFactory.Context>
{
    public OmniHashBuilderOperatorFactory(
            VecType[] buildTypes,
            int[] buildOutputCols,
            int[] buildHashCols,
            int operatorCount)
    {
        super(new Context(buildTypes, buildOutputCols, buildHashCols, operatorCount));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createHashBuilderOperatorFactory(
                OmniUtils.transformVecType(context.buildTypes),
                context.buildOutputCols,
                context.buildHashCols,
                context.operatorCount);
    }

    private static native long createHashBuilderOperatorFactory(int[] buildTypes, int[] buildOutputCols, int[] buildHashCols, int operatorCount);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] buildTypes;
        private final int[] buildOutputCols;
        private final int[] buildHashCols;
        private final int operatorCount;

        public Context(VecType[] buildTypes,
                int[] buildOutputCols,
                int[] buildHashCols,
                int operatorCount)
        {
            this.buildTypes = requireNonNull(buildTypes, "buildTypes");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildHashCols = requireNonNull(buildHashCols, "buildHashCols");
            this.operatorCount = operatorCount;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(buildTypes, buildOutputCols, buildHashCols);
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
            return Objects.equals(buildTypes, that.buildTypes)
                    && Objects.equals(buildOutputCols, that.buildOutputCols)
                    && Objects.equals(buildHashCols, that.buildHashCols);
        }
    }
}
