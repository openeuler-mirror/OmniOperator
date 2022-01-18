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
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni aggregation with expression operator factory.
 *
 * @since 20211021
 */
public class OmniHashAggregationWithExprOperatorFactory
        extends
            OmniOperatorFactory<OmniHashAggregationWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash aggregation with expression operator factory.
     *
     * @param groupByChanel the group by chanel
     * @param aggChannels the agg channels
     * @param sourceTypes the source types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param inputRaw the input raw
     * @param outputPartial the output partial
     */
    public OmniHashAggregationWithExprOperatorFactory(String[] groupByChanel, String[] aggChannels,
            VecType[] sourceTypes, FunctionType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw,
            boolean outputPartial) {
        super(new FactoryContext(new JitContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes,
                aggOutputTypes, inputRaw, outputPartial)));
    }

    private static native long createHashAggregationWithExprOperatorFactory(String[] groupByChanel,
            String[] aggChannels, String sourceTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean inputRaw,
            boolean outputPartial, long JitContext);

    private static native long createHashAggregationWithExprJitContext(String[] groupByChanel, String[] aggChannels,
            String sourceTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean inputRaw, boolean outputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashAggregationWithExprOperatorFactory(context.groupByChanel, context.aggChannels,
                VecTypeSerializer.serialize(context.sourceTypes), toNativeConstants(context.aggFunctionTypes),
                VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial,
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

        private final VecType[] sourceTypes;

        private final FunctionType[] aggFunctionTypes;

        private final VecType[] aggOutputTypes;

        private final boolean inputRaw;

        private final boolean outputPartial;

        /**
         * Instantiates a new Context.
         *
         * @param groupByChanel the group by chanel
         * @param aggChannels the agg channels
         * @param sourceTypes the source types
         * @param aggFunctionTypes the agg function types
         * @param aggOutputTypes the agg output types
         * @param inputRaw the input raw
         * @param outputPartial the output partial
         */
        public JitContext(String[] groupByChanel, String[] aggChannels, VecType[] sourceTypes,
                FunctionType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw, boolean outputPartial) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(aggChannels),
                    Arrays.hashCode(sourceTypes), Arrays.hashCode(aggFunctionTypes), inputRaw, outputPartial);
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
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes) && inputRaw == that.inputRaw
                    && outputPartial == that.outputPartial;
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
                    VecTypeSerializer.serialize(context.sourceTypes), toNativeConstants(context.aggFunctionTypes),
                    VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial);
        }
    }
}
