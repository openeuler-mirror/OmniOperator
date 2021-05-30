package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class JOmniSortOperator
        extends JOmniOperator
{
    public static class JOmniSortOperatorFactory
            extends JOmniOperatorFactory
    {
        private static final Map<Integer, Long> omniSortCompilers = new ConcurrentHashMap<>();
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
            long nativeOperatorFactory;
            int key = Objects.hash(sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts);
            if (omniSortCompilers.containsKey(key)) {
                nativeOperatorFactory = omniSortCompilers.get(key);
            }
            else {
                // compile and optimized
                nativeOperatorFactory = getJniWrapper().createSortOperatorFactory(
                        sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts);
            }

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
            long nativeOperator = jniWrapper.createOperator(getNativeOperatorFactory());
            JOmniOperator jOmniOperator = new JOmniSortOperator(jniWrapper, nativeOperator);
            return jOmniOperator;
        }
    }

    public JOmniSortOperator(JniWrapper jniWrapper, long nativeOperator)
    {
        super(jniWrapper, nativeOperator);
    }

    @Override
    public int addInput(List<Vec> datas, int[] positionCounts)
    {
        int vecCount = datas.size();
        int pageCount = positionCounts.length;

        if (vecCount == 0 || pageCount == 0) {
            return 0;
        }

        int columnCount = vecCount / pageCount;

        LongVec datasAddrVec = OmniUtils.transformVecAddress(datas);
        IntVec rowCountsAddrVec = OmniUtils.getRowNumbers(datas, columnCount);
        getJniWrapper().addInput(getNativeOperator(), datasAddrVec.getAddress(), vecCount, rowCountsAddrVec.getAddress(), pageCount);

        datasAddrVec.close();
        rowCountsAddrVec.close();
        return 0;
    }

    @Override
    public int addInput(List<Vec> data, int positionCounts)
    {
        return 0;
    }

    @Override
    public OMResult[] getOutput() {
        long nativeOperator = getNativeOperator();
        OMResult[] results = getJniWrapper().getOutput(nativeOperator);
        return results;
    }

    @Override
    public void close() {
        getJniWrapper().close(getNativeOperator());
    }
}