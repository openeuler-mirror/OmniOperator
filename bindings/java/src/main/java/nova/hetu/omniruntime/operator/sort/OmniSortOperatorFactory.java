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

    private static native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols, int[] ascendings, int[] nullFirsts);
}
