package nova.hetu.omniruntime.operator.sort;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Arrays;
import java.util.Objects;

public class OmniSortOperatorFactory
        extends OmniOperatorFactory<OmniSortOperatorFactory.Context>
{
    public OmniSortOperatorFactory(VecType[] sourceTypes,
            int[] outputColumns,
            int[] sortColumns,
            int[] sortAscendings,
            int[] sortNullFirsts)
    {
        super(new Context(sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createSortOperatorFactory(
                OmniUtils.transformVecType(context.sourceTypes),
                context.outputColumns,
                context.sortColumns,
                context.sortAscendings,
                context.sortNullFirsts);
    }

    private static native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols, int[] ascendings, int[] nullFirsts);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] sourceTypes;
        private final int[] outputColumns;
        private final int[] sortColumns;
        private final int[] sortAscendings;
        private final int[] sortNullFirsts;

        public Context(VecType[] sourceTypes,
                int[] outputColumns,
                int[] sortColumns,
                int[] sortAscendings,
                int[] sortNullFirsts)
        {
            this.sourceTypes = sourceTypes;
            this.outputColumns = outputColumns;
            this.sortColumns = sortColumns;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputColumns), Arrays.hashCode(sortColumns), Arrays.hashCode(sortAscendings), Arrays.hashCode(sortNullFirsts));
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
            return Arrays.equals(sourceTypes, that.sourceTypes)
                    && Arrays.equals(outputColumns, that.outputColumns)
                    && Arrays.equals(sortColumns, that.sortColumns)
                    && Arrays.equals(sortAscendings, that.sortAscendings)
                    && Arrays.equals(sortNullFirsts, that.sortNullFirsts);
        }
    }
}
