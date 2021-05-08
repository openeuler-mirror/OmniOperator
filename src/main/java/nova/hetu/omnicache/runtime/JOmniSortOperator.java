package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.Vec;

import java.util.List;

public class JOmniSortOperator {
    public static class JOmniSortOperatorFactory
    {
        private final JniWrapper jniWrapper = new JniWrapper();
        private final int[] sourceTypes;
        private final int[] outputColumns;
        private final int[] sortColumns;
        private final int[] sortAscendings;
        private final int[] sortNullFirsts;
        long nativeOperatorFactory;

        public static JOmniSortOperatorFactory createJOmniSortOperatorFactory(
                int[] sourceTypes,
                int[] outputColumns,
                int[] sortColumns,
                int[] sortAscendings,
                int[] sortNullFirsts)
        {
            // compile and optimized
            long nativeOperatorFactory = 0;

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
            this.sourceTypes = sourceTypes;
            this.outputColumns = outputColumns;
            this.sortColumns = sortColumns;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
            this.nativeOperatorFactory = nativeOperatorFactory;
        }

        public JOmniSortOperator createJOmniOperator(JniWrapper jniWrapper, long nativeOperatorFactory)
        {
            long nativeOperator = 0;
            return new JOmniSortOperator(jniWrapper, nativeOperator);
        }
    }

    private final JniWrapper jniWrapper;
    private final long nativeOperator;

    public JOmniSortOperator(JniWrapper jniWrapper, long nativeOperator)
    {
        this.jniWrapper = jniWrapper;
        this.nativeOperator = nativeOperator;
    }

    public int addInput(long nativeOperator, List<Vec> datas, int pageCount)
    {
        return 0;
    }

    public OMResult getOutput(long nativeOperator)
    {
        OMResult result = new OMResult();
        return result;
    }
}
