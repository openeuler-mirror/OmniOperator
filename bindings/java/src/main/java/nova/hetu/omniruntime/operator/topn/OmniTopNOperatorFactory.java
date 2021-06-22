package nova.hetu.omniruntime.operator.topn;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

public class OmniTopNOperatorFactory
        extends OmniOperatorFactory<OmniTopNOperatorFactory.Context>
{
    public OmniTopNOperatorFactory(
            VecType[] sourceTypes,
            int n,
            int[] sortCols,
            int[] sortAssendings,
            int[] sortNullFirsts)
    {
        super(new Context(sourceTypes, n, sortCols, sortAssendings, sortNullFirsts));
    }

    private static native long createTopNOperatorFactory(int[] sourceTypes, int n, int[] sortCols, int[] sortAssendings, int[] sortNullFirsts);

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createTopNOperatorFactory(
                toNativeConstants(context.sourceTypes),
                context.n,
                context.sortCols,
                context.sortAssendings,
                context.sortNullFirsts);
    }

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] sourceTypes;
        private final int n;
        private final int[] sortCols;
        private final int[] sortAssendings;
        private int[] sortNullFirsts;

        public Context(
                VecType[] sourceTypes,
                int n,
                int[] sortCols,
                int[] sortAssendings,
                int[] sortNullFirsts)
        {
            this.sourceTypes = sourceTypes;
            this.n = n;
            this.sortCols = sortCols;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceTypes, n, sortCols, sortAssendings, sortNullFirsts);
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
            if (!super.equals(o)) {
                return false;
            }
            Context context = (Context) o;
            return n == context.n && Arrays.equals(sourceTypes, context.sourceTypes) && Arrays.equals(sortCols, context.sortCols) && Arrays.equals(sortAssendings, context.sortAssendings) && Arrays.equals(sortNullFirsts, context.sortNullFirsts);
        }
    }
}
