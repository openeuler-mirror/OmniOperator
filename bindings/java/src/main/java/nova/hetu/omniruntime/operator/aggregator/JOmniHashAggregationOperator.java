package nova.hetu.omniruntime.operator.aggregator;

import nova.hetu.omniruntime.operator.JOmniOperator;
import nova.hetu.omniruntime.operator.JOmniOperatorFactory;
import nova.hetu.omniruntime.operator.JniWrapper;
import nova.hetu.omniruntime.operator.OMResult;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.*;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

public class JOmniHashAggregationOperator extends JOmniOperator {

    public JOmniHashAggregationOperator(JniWrapper jniWrapper, long nativeOperator) {
        super(jniWrapper, nativeOperator);
    }
    @Override
    public int addInput(List<Vec> data, int[] positionCounts) {
        IntVec inputRowSize = null;
        int columnCount = data.size() / positionCounts.length;
        LongVec inputDataAddr = null;
        try {
            inputDataAddr = OmniUtils.transformVecAddress(data);
            inputRowSize = OmniUtils.getRowNumbers(data, columnCount);
            getJniWrapper().addInput(getNativeOperator(), inputDataAddr.getAddress(), inputDataAddr.size(), inputRowSize.getAddress(), inputRowSize.size());
            return 0;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("execute agg intermediate failed.", e);
        } finally {
            if (inputRowSize != null) {
                inputRowSize.close();
            }
            if (inputDataAddr != null) {
                inputDataAddr.close();
            }
        }
    }

    @Override
    public int addInput(List<Vec> data, int positionCount) {
        return 0;
    }

    @Override
    public OMResult[] getOutput() {
        return getJniWrapper().getOutput(getNativeOperator());
    }

    @Override
    public void close() {
        getJniWrapper().close(getNativeOperator());
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
                        groupByChanel, OmniUtils.transformVecType(groupByTypes), aggChannels, OmniUtils.transformVecType(aggTypes), OmniUtils.transformAggType(aggFunctionTypes), OmniUtils.transformVecType(aggOutputTypes));
                if (nativeOperatorFactory == null) {
                    throw new RuntimeException(format("create nativeOperatorFactory failed"));
                }
                getOmniFactoryCache().put(hashKey, nativeOperatorFactory);
            }

            return new JOmniHashAggregationOperator.JOmniHashAggregationOperatorFactory(
                    groupByChanel,
                    OmniUtils.transformVecType(groupByTypes),
                    aggChannels,
                    OmniUtils.transformVecType(aggTypes),
                    OmniUtils.transformAggType(aggFunctionTypes),
                    OmniUtils.transformVecType(aggOutputTypes),
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
            long nativeOperator = jniWrapper.createOperator(getNativeOperatorFactory());
            JOmniOperator jOmniOperator = new JOmniHashAggregationOperator(jniWrapper, nativeOperator);
            return jOmniOperator;
        }
    }
}
