package nova.hetu.omniruntime.operator.sort;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;

import java.util.Objects;

public class OmniSortOperatorFactory
        extends OmniOperatorFactory
{
    private final int[] sourceTypes;
    private final int[] outputColumns;
    private final int[] sortColumns;
    private final int[] sortAscendings;
    private final int[] sortNullFirsts;

    public OmniSortOperatorFactory(int[] sourceTypes,
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
    protected long createNativeOperatorFactory()
    {
        return createSortOperatorFactory(sourceTypes,
                outputColumns,
                sortColumns,
                sortAscendings,
                sortNullFirsts);
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
        OmniSortOperatorFactory that = (OmniSortOperatorFactory) o;
        return Objects.equals(sourceTypes, that.sourceTypes)
                && Objects.equals(outputColumns, that.outputColumns)
                && Objects.equals(sortColumns, that.sortColumns)
                && Objects.equals(sortAscendings, that.sortAscendings)
                && Objects.equals(sortNullFirsts, that.sortNullFirsts);
    }

    private static native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols, int[] ascendings, int[] nullFirsts);
}
