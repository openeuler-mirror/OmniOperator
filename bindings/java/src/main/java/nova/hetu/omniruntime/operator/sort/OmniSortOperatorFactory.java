package nova.hetu.omniruntime.operator.sort;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Objects;

public class OmniSortOperatorFactory
        extends OmniOperatorFactory<OmniSortOperatorFactory.Context>
{
    public OmniSortOperatorFactory(int[] sourceTypes,
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
        return createSortOperatorFactory(context.sourceTypes,
                context.outputColumns,
                context.sortColumns,
                context.sortAscendings,
                context.sortNullFirsts);
    }

    private static native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols, int[] ascendings, int[] nullFirsts);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final int[] sourceTypes;
        private final int[] outputColumns;
        private final int[] sortColumns;
        private final int[] sortAscendings;
        private final int[] sortNullFirsts;

        public Context(int[] sourceTypes,
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
            return Objects.hash(sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts);
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
            return Objects.equals(sourceTypes, that.sourceTypes)
                    && Objects.equals(outputColumns, that.outputColumns)
                    && Objects.equals(sortColumns, that.sortColumns)
                    && Objects.equals(sortAscendings, that.sortAscendings)
                    && Objects.equals(sortNullFirsts, that.sortNullFirsts);
        }
    }
}
