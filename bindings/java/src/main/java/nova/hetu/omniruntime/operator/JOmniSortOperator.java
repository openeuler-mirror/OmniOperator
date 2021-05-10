package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.Vec;

import java.util.List;

public class JOmniSortOperator
        extends JOmniOperator
{
    public static class JOmniSortOperatorFactory
            extends JOmniOperatorFactory
    {
        private final int[] sourceTypes;
        private final int[] outputColumns;
        private final int[] sortColumns;
        private final int[] sortAscendings;
        private final int[] sortNullFirsts;

        public static JOmniSortOperatorFactory createJOmniSortOperatorFactory(
                int[] sourceTypes,
                int[] outputColumns,
                int[] sortColumns,
                int[] sortAscendings,
                int[] sortNullFirsts)
        {
            // compile and optimized
            long nativeOperatorFactory = getJniWrapper().createSortOperatorFactory(
                    sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts);

            return new JOmniSortOperatorFactory(
                    sourceTypes,
                    outputColumns,
                    sortColumns,
                    sortAscendings,
                    sortNullFirsts,
                    nativeOperatorFactory);
        }

        public JOmniSortOperatorFactory(
                int[] sourceTypes,
                int[] outputColumns,
                int[] sortColumns,
                int[] sortAscendings,
                int[] sortNullFirsts,
                long nativeOperatorFactory)
        {
            super(nativeOperatorFactory);
            this.sourceTypes = sourceTypes;
            this.outputColumns = outputColumns;
            this.sortColumns = sortColumns;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public JOmniOperator createOmniOperator()
        {
            JniWrapper jniWrapper = getJniWrapper();
            long nativeOperator = jniWrapper.createSortOperator(getNativeOperatorFactory());
            JOmniOperator jOmniOperator = new JOmniSortOperator(jniWrapper, nativeOperator);
            return jOmniOperator;
        }
    }

    public JOmniSortOperator(JniWrapper jniWrapper, long nativeOperator)
    {
        super(jniWrapper, nativeOperator);
    }

    @Override
    public int addInput(List<Vec> datas, int[] positionCounts, int pageCount)
    {
        int vecSize = datas.size();
        long[] dataAddrs = new long[vecSize];
        for (int i = 0; i < vecSize; i++) {
            dataAddrs[i] = datas.get(i).getAddress();
        }

        long nativeOperator = getNativeOperator();
        getJniWrapper().addSortInput(nativeOperator, dataAddrs, positionCounts, pageCount);
        return 0;
    }

    @Override
    public OMResult[] getOutput() {
        long nativeOperator = getNativeOperator();
        OMResult[] results = getJniWrapper().getSortOutput(nativeOperator);
        return results;
    }
}