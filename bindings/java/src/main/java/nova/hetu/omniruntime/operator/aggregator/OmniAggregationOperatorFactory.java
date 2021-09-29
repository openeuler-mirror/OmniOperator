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
     * @param aggTypes the agg types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param inputRaw the input raw
     * @param outputPartial the output partial
     */
    public OmniAggregationOperatorFactory(VecType[] aggTypes, AggType[] aggFunctionTypes, VecType[] aggOutputTypes,
        boolean inputRaw, boolean outputPartial) {
        super(new FactoryContext(new JitContext(aggTypes, aggFunctionTypes, aggOutputTypes, inputRaw, outputPartial)));
    }

    private static native long createAggregationOperatorFactory(String aggTypes, int[] aggFunctionTypes,
        String aggOutputTypes, boolean inputRaw, boolean outputPartial, long jitContext);

    private static native long createAggregationJitContext(String aggTypes, int[] aggFunctionTypes,
        String aggOutputTypes, boolean inputRaw, boolean outputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createAggregationOperatorFactory(VecTypeSerializer.serialize(context.aggTypes),
            toNativeConstants(context.aggFunctionTypes), VecTypeSerializer.serialize(context.aggOutputTypes),
            context.inputRaw, context.outputPartial, factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] aggTypes;

        private final AggType[] aggFunctionTypes;

        private final VecType[] aggOutputTypes;

        private final boolean inputRaw;

        private final boolean outputPartial;

        /**
         * Instantiates a new Context.
         *
         * @param aggTypes the agg types
         * @param aggFunctionTypes the agg function types
         * @param aggOutputTypes the agg output types
         * @param inputRaw the input raw
         * @param outputPartial the output partial
         */
        public JitContext(VecType[] aggTypes, AggType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw,
            boolean outputPartial) {
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.inputRaw = requireNonNull(inputRaw, "inputRaw");
            this.outputPartial = requireNonNull(outputPartial, "outputPartial");
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(aggTypes), Arrays.hashCode(aggFunctionTypes));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OmniAggregationOperatorFactory.JitContext that = (OmniAggregationOperatorFactory.JitContext) o;
            return Arrays.equals(aggTypes, that.aggTypes) && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes);
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
            long aggregationJitContext = createAggregationJitContext(VecTypeSerializer.serialize(context.aggTypes),
                toNativeConstants(context.aggFunctionTypes), VecTypeSerializer.serialize(context.aggOutputTypes),
                context.inputRaw, context.outputPartial);
            return aggregationJitContext;
        }
    }
}
