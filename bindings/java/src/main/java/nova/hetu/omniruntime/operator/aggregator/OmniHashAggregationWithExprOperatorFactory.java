/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.aggregator;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni aggregation with expression operator factory.
 *
 * @since 2021-10-21
 */
public class OmniHashAggregationWithExprOperatorFactory
        extends OmniOperatorFactory<OmniHashAggregationWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash aggregation with expression operator factory.
     *
     * @param groupByChanel the group by chanel
     * @param aggChannels the agg channels
     * @param sourceTypes the source types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param isInputRaw the input raw
     * @param isOutputPartial the output partial
     * @param operatorConfig the operator config
     */
    public OmniHashAggregationWithExprOperatorFactory(String[] groupByChanel, String[] aggChannels,
            DataType[] sourceTypes, FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial, OperatorConfig operatorConfig) {
        super(new FactoryContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, isInputRaw,
                isOutputPartial, operatorConfig));
    }

    /**
     * Instantiates a new Omni hash aggregation with expression operator factory.
     *
     * @param groupByChanel the group by chanel
     * @param aggChannels the agg channels
     * @param sourceTypes the source types
     * @param aggFunctionTypes the agg function types
     * @param maskChannels mask channel list for aggregators
     * @param aggOutputTypes the agg output types
     * @param isInputRaw the input raw
     * @param isOutputPartial the output partial
     * @param operatorConfig the operator config
     */
    public OmniHashAggregationWithExprOperatorFactory(String[] groupByChanel, String[] aggChannels,
            DataType[] sourceTypes, FunctionType[] aggFunctionTypes, int[] maskChannels, DataType[] aggOutputTypes,
            boolean isInputRaw, boolean isOutputPartial, OperatorConfig operatorConfig) {
        super(new FactoryContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, maskChannels,
                aggOutputTypes, isInputRaw, isOutputPartial, operatorConfig));
    }

    /**
     * Instantiates a new Omni hash aggregation with expression operator factory
     * with jit default.
     *
     * @param groupByChanel the group by chanel
     * @param aggChannels the agg channels
     * @param sourceTypes the source types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param isInputRaw the input raw
     * @param isOutputPartial the output partial
     */
    public OmniHashAggregationWithExprOperatorFactory(String[] groupByChanel, String[] aggChannels,
            DataType[] sourceTypes, FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial) {
        this(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, isInputRaw, isOutputPartial,
                new OperatorConfig());
    }

    private static native long createHashAggregationWithExprOperatorFactory(String[] groupByChanel,
            String[] aggChannels, String sourceTypes, int[] aggFunctionTypes, int[] maskChannels, String aggOutputTypes,
            boolean isInputRaw, boolean isOutputPartial, String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createHashAggregationWithExprOperatorFactory(context.groupByChanel, context.aggChannels,
                DataTypeSerializer.serialize(context.sourceTypes), toNativeConstants(context.aggFunctionTypes),
                context.maskChannels, DataTypeSerializer.serialize(context.aggOutputTypes), context.isInputRaw,
                context.isOutputPartial, OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * The type Factory context.
     *
     * @since 20210630
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private static final int INVALID_MASK_CHANNEL = -1;

        private final String[] groupByChanel;

        private final String[] aggChannels;

        private final DataType[] sourceTypes;

        private final FunctionType[] aggFunctionTypes;

        private final int[] maskChannels;

        private final DataType[] aggOutputTypes;

        private final boolean isInputRaw;

        private final boolean isOutputPartial;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param groupByChanel the group by chanel
         * @param aggChannels the agg channels
         * @param sourceTypes the source types
         * @param aggFunctionTypes the agg function types
         * @param maskChannels mask channel list for aggregators
         * @param aggOutputTypes the agg output types
         * @param isInputRaw the input raw
         * @param isOutputPartial the output partial
         * @param operatorConfig the operator config
         */
        public FactoryContext(String[] groupByChanel, String[] aggChannels, DataType[] sourceTypes,
                FunctionType[] aggFunctionTypes, int[] maskChannels, DataType[] aggOutputTypes, boolean isInputRaw,
                boolean isOutputPartial, OperatorConfig operatorConfig) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.maskChannels = requireNonNull(maskChannels, "maskChannels is null");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.isInputRaw = isInputRaw;
            this.isOutputPartial = isOutputPartial;
            this.operatorConfig = operatorConfig;
        }

        /**
         * Instantiates a new Context.
         *
         * @param groupByChanel the group by chanel
         * @param aggChannels the agg channels
         * @param sourceTypes the source types
         * @param aggFunctionTypes the agg function types
         * @param aggOutputTypes the agg output types
         * @param isInputRaw the input raw
         * @param isOutputPartial the output partial
         * @param operatorConfig the operator config
         */
        public FactoryContext(String[] groupByChanel, String[] aggChannels, DataType[] sourceTypes,
                FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw, boolean isOutputPartial,
                OperatorConfig operatorConfig) {
            this(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, getDefaultMaskChannel(aggFunctionTypes),
                    aggOutputTypes, isInputRaw, isOutputPartial, operatorConfig);
        }

        private static int[] getDefaultMaskChannel(FunctionType[] aggFunctionTypes) {
            int[] maskChannelArray = new int[aggFunctionTypes.length]; // one mask channel for each function
            Arrays.fill(maskChannelArray, INVALID_MASK_CHANNEL);
            return maskChannelArray;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(aggChannels),
                    Arrays.hashCode(sourceTypes), Arrays.hashCode(aggFunctionTypes), Arrays.hashCode(maskChannels),
                    Arrays.hashCode(aggOutputTypes), isInputRaw, isOutputPartial, operatorConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FactoryContext that = (FactoryContext) obj;
            return Arrays.equals(groupByChanel, that.groupByChanel) && Arrays.equals(aggChannels, that.aggChannels)
                    && Arrays.equals(sourceTypes, that.sourceTypes)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes)
                    && Arrays.equals(maskChannels, that.maskChannels)
                    && Arrays.equals(aggOutputTypes, that.aggOutputTypes) && isInputRaw == that.isInputRaw
                    && (isOutputPartial == that.isOutputPartial) && operatorConfig.equals(that.operatorConfig);
        }
    }
}
