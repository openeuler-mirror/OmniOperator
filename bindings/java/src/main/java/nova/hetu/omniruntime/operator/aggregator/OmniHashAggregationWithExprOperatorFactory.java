/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.aggregator;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
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
     */
    public OmniHashAggregationWithExprOperatorFactory(String[] groupByChanel, String[] aggChannels,
            DataType[] sourceTypes, FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial) {
        super(new FactoryContext(new JitContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes,
                aggOutputTypes, isInputRaw, isOutputPartial)));
    }

    private static native long createHashAggregationWithExprOperatorFactory(String[] groupByChanel,
            String[] aggChannels, String sourceTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial, long jitContext);

    private static native long createHashAggregationWithExprJitContext(String[] groupByChanel, String[] aggChannels,
            String sourceTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashAggregationWithExprOperatorFactory(context.groupByChanel, context.aggChannels,
                DataTypeSerializer.serialize(context.sourceTypes), toNativeConstants(context.aggFunctionTypes),
                DataTypeSerializer.serialize(context.aggOutputTypes), context.isInputRaw, context.isOutputPartial,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20211021
     */
    public static class JitContext implements OmniJitContext {
        private final String[] groupByChanel;

        private final String[] aggChannels;

        private final DataType[] sourceTypes;

        private final FunctionType[] aggFunctionTypes;

        private final DataType[] aggOutputTypes;

        private final boolean isInputRaw;

        private final boolean isOutputPartial;

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
         */
        public JitContext(String[] groupByChanel, String[] aggChannels, DataType[] sourceTypes,
                FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
                boolean isOutputPartial) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.isInputRaw = isInputRaw;
            this.isOutputPartial = isOutputPartial;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(aggChannels),
                    Arrays.hashCode(sourceTypes), Arrays.hashCode(aggFunctionTypes), isInputRaw, isOutputPartial);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext that = (JitContext) obj;
            return Arrays.equals(groupByChanel, that.groupByChanel) && Arrays.equals(sourceTypes, that.sourceTypes)
                    && Arrays.equals(aggChannels, that.aggChannels)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes) && isInputRaw == that.isInputRaw
                    && isOutputPartial == that.isOutputPartial;
        }
    }

    /**
     * The type Factory context.
     *
     * @since 20210630
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         */
        public FactoryContext(JitContext jitContext) {
            super(jitContext);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return createHashAggregationWithExprJitContext(context.groupByChanel, context.aggChannels,
                    DataTypeSerializer.serialize(context.sourceTypes), toNativeConstants(context.aggFunctionTypes),
                    DataTypeSerializer.serialize(context.aggOutputTypes), context.isInputRaw, context.isOutputPartial);
        }
    }
}
