/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.aggregator;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni aggregation operator factory.
 *
 * @since 20210630
 */
public class OmniAggregationOperatorFactory extends OmniOperatorFactory<OmniAggregationOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni aggregation operator factory.
     *
     * @param sourceTypes the aggregation source types
     * @param aggFunctionTypes the aggregation function types
     * @param aggInputChannels the aggregation function input channels
     * @param aggOutputTypes the aggregation output types
     * @param inputRaw the input raw
     * @param outputPartial the output partial
     */
    public OmniAggregationOperatorFactory(VecType[] sourceTypes, AggType[] aggFunctionTypes, int[] aggInputChannels,
            VecType[] aggOutputTypes, boolean inputRaw, boolean outputPartial) {
        super(new FactoryContext(new JitContext(sourceTypes, aggFunctionTypes, aggInputChannels, aggOutputTypes,
                inputRaw, outputPartial)));
    }

    private static native long createAggregationOperatorFactory(String sourceTypes, int[] aggFunctionTypes,
            int[] aggInputChannels, String aggOutputTypes, boolean inputRaw, boolean outputPartial, long jitContext);

    private static native long createAggregationJitContext(String sourceTypes, int[] aggFunctionTypes,
            int[] aggInputChannels, String aggOutputTypes, boolean inputRaw, boolean outputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createAggregationOperatorFactory(VecTypeSerializer.serialize(context.sourceTypes),
                toNativeConstants(context.aggFunctionTypes), context.aggInputChannels,
                VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] sourceTypes;

        private final AggType[] aggFunctionTypes;

        private final int[] aggInputChannels;

        private final VecType[] aggOutputTypes;

        private final boolean inputRaw;

        private final boolean outputPartial;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param aggFunctionTypes the aggregation function types
         * @param aggInputChannels the aggregation input channels
         * @param aggOutputTypes the aggregation output types
         * @param inputRaw the input raw
         * @param outputPartial the output partial
         */
        public JitContext(VecType[] sourceTypes, AggType[] aggFunctionTypes, int[] aggInputChannels,
                VecType[] aggOutputTypes, boolean inputRaw, boolean outputPartial) {
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes is null");
            this.aggInputChannels = requireNonNull(aggInputChannels, "aggInputChannels is null");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes is null");
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(aggFunctionTypes),
                    Arrays.hashCode(aggInputChannels), Arrays.hashCode(aggOutputTypes), inputRaw, outputPartial);
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
            return Arrays.equals(sourceTypes, that.sourceTypes)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes)
                    && Arrays.equals(aggInputChannels, that.aggInputChannels)
                    && Arrays.equals(aggOutputTypes, that.aggOutputTypes) && inputRaw == that.inputRaw
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
            long aggregationJitContext = createAggregationJitContext(VecTypeSerializer.serialize(context.sourceTypes),
                    toNativeConstants(context.aggFunctionTypes), context.aggInputChannels,
                    VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial);
            return aggregationJitContext;
        }
    }
}
