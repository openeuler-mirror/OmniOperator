package nova.hetu.omniruntime.operator.aggregator;

import nova.hetu.omniruntime.operator.JOmniOperator;
import nova.hetu.omniruntime.operator.JOmniOperatorFactory;
import nova.hetu.omniruntime.operator.JniWrapper;
import nova.hetu.omniruntime.operator.OMResult;
import nova.hetu.omniruntime.vector.AggType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecType;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

public class JOmniHashAggregationOperator extends JOmniOperator {

    public JOmniHashAggregationOperator(JniWrapper jniWrapper, long nativeOperator) {
        super(jniWrapper, nativeOperator);
    }
    @Override
    public int addInput(List<Vec> data, int[] positionCounts, VecType[] types) {
        return 0;
    }
    private LongVec transformVecAddress(List<Vec> inputs) {
        LongVec address = new LongVec(inputs.size());
        for (int idx = 0; idx < inputs.size(); idx++) {
            address.set(idx, inputs.get(idx).getAddress());
        }
        return address;
    }

    private static int[] transformVecType(VecType[] vecTypes)
    {
        int[] vecTypeValue = new int[vecTypes.length];
        for (int idx = 0; idx < vecTypes.length; idx++) {
            vecTypeValue[idx] = vecTypes[idx].getValue();
        }
        return vecTypeValue;
    }

    private static int[] transformAggType(AggType[] aggTypes)
    {
        int[] aggTypeValue = new int[aggTypes.length];
        for (int idx = 0; idx < aggTypes.length; idx++) {
            aggTypeValue[idx] = aggTypes[idx].getValue();
        }
        return aggTypeValue;
    }

    private IntVec getRowNumbers(List<Vec> inputs, int columnCount) {
        int totalColumn = inputs.size();
        if (totalColumn % columnCount != 0) {
            throw new IllegalArgumentException(format("input vec error:total colum: %s,column count: %s", totalColumn, columnCount));
        }

        int pageNum = totalColumn / columnCount;
        IntVec rowNums = new IntVec(pageNum);
        for (int idx = 0; idx < pageNum; idx++) {
            rowNums.set(idx, inputs.get(idx * columnCount).size());
        }
        return rowNums;
    }

    public int transformPrepareInfoToVec(IntVec prepareInfo, int[] values, int offset)
    {
        for (int value : values) {
            prepareInfo.set(offset++, value);
        }
        return offset;
    }

    @Override
    public int addInput(List<Vec> data, int positionCount, VecType[] vecTypes)
    {
        LongVec inputDataAddr = null;
        IntVec inputRowSize = null;
        IntVec inputVecTypes = null;
        try {
            inputDataAddr = transformVecAddress(data);
            inputRowSize = getRowNumbers(data, vecTypes.length);
            int[] inputTypes = transformVecType(vecTypes);
            inputVecTypes = new IntVec(inputTypes.length);
            transformPrepareInfoToVec(inputVecTypes, inputTypes, 0);

            // TODO use uified addInput
            getJniWrapper().executeAggIntermediate(
                    getNativeOperator(),
                    inputDataAddr.getAddress(),
                    inputDataAddr.size(),
                    vecTypes.length,
                    inputRowSize.getAddress(),
                    inputRowSize.size(),
                    inputVecTypes.getAddress());
            return 0;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("execute agg intermediate failed.", e);
        } finally {
            if (inputDataAddr != null) {
                inputDataAddr.close();
            }
            if (inputRowSize != null) {
                inputRowSize.close();
            }
            if (inputVecTypes != null) {
                inputVecTypes.close();
            }
        }
    }

    @Override
    public OMResult[] getOutput() {
        return getJniWrapper().getOutput(getNativeOperator());
    }

    public static class JOmniHashAggregationOperatorFactory
            extends JOmniOperatorFactory {
        private final int[] groupByChanel;
        private final int[] groupByTypes;
        private final int[] aggChannels;
        private final int[] aggTypes;
        private final int[] aggFunctionTypes;
        private final int[] aggOutputTypes;

        public static JOmniHashAggregationOperatorFactory createJOmniHashAggregationOperatorFactory(
                int[] groupByChanel,
                VecType[] groupByTypes,
                int[] aggChannels,
                VecType[] aggTypes,
                AggType[] aggFunctionTypes,
                VecType[] aggOutputTypes)
        {
            // compile and optimized
            Integer hashKey = Objects.hash(groupByTypes, aggTypes, aggFunctionTypes);
            Long nativeOperatorFactory = getOmniFactoryCache().getIfPresent(hashKey);
            if (nativeOperatorFactory == null) {
                nativeOperatorFactory = getJniWrapper().createHashAggregationOperatorFactory(
                        groupByChanel, transformVecType(groupByTypes), aggChannels, transformVecType(aggTypes), transformAggType(aggFunctionTypes), transformVecType(aggOutputTypes));
                if (nativeOperatorFactory == null) {
                    throw new RuntimeException(format("create nativeOperatorFactory failed"));
                }
                getOmniFactoryCache().put(hashKey, nativeOperatorFactory);
            }

            return new JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory(
                    groupByChanel,
                    transformVecType(groupByTypes),
                    aggChannels,
                    transformVecType(aggTypes),
                    transformAggType(aggFunctionTypes),
                    transformVecType(aggOutputTypes),
                    nativeOperatorFactory);
        }

        public JOmniHashAggregationOperatorFactory(
                int[] groupByChanel,
                int[] groupByTypes,
                int[] aggChannels,
                int[] aggTypes,
                int[] aggFunctionTypes,
                int[] aggOutputTypes,
                long nativeOperatorFactory)
        {
            super(nativeOperatorFactory);
            this.groupByChanel = groupByChanel;
            this.groupByTypes = groupByTypes;
            this.aggChannels = aggChannels;
            this.aggTypes = aggTypes;
            this.aggFunctionTypes = aggFunctionTypes;
            this.aggOutputTypes = aggOutputTypes;
        }

        @Override
        public JOmniOperator createOmniOperator() {
            JniWrapper jniWrapper = getJniWrapper();
            long nativeOperator = jniWrapper.createOperator(getNativeOperatorFactory(), JniWrapper.OperatorType.HASH_AGGREGATION.getValue());
            JOmniOperator jOmniOperator = new JOmniHashAggregationOperator(jniWrapper, nativeOperator);
            return jOmniOperator;
        }
    }
}
