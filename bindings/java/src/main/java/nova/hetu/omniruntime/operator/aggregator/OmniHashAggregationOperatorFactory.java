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
 * The type Omni hash aggregation operator factory.
 *
 * @since 20210630
 */
public class OmniHashAggregationOperatorFactory
        extends
            OmniOperatorFactory<OmniHashAggregationOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash aggregation operator factory.
     *
     * @param groupByChanel the group by chanel
     * @param groupByTypes the group by types
     * @param aggChannels the agg channels
     * @param aggTypes the agg types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param inputRaw the input raw
     * @param outputPartial the output partial
     */
    public OmniHashAggregationOperatorFactory(String[] groupByChanel, VecType[] groupByTypes, String[] aggChannels,
            VecType[] aggTypes, FunctionType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw,
            boolean outputPartial) {
        super(new FactoryContext(new JitContext(groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes,
                aggOutputTypes, inputRaw, outputPartial)));
    }

    private static native long createHashAggregationOperatorFactory(String[] groupByChanel, String groupByTypes,
            String[] aggChannels, String aggTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean inputRaw,
            boolean outputPartial, long JitContext);

    private static native long createHashAggregationJitContext(String[] groupByChanel, String groupByTypes,
            String[] aggChannels, String aggTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean inputRaw,
            boolean outputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashAggregationOperatorFactory(context.groupByChanel,
                VecTypeSerializer.serialize(context.groupByTypes), context.aggChannels,
                VecTypeSerializer.serialize(context.aggTypes), toNativeConstants(context.aggFunctionTypes),
                VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final String[] groupByChanel;

        private final VecType[] groupByTypes;

        private final String[] aggChannels;

        private final VecType[] aggTypes;

        private final FunctionType[] aggFunctionTypes;

        private final VecType[] aggOutputTypes;

        private final boolean inputRaw;

        private final boolean outputPartial;

        /**
         * Instantiates a new Context.
         *
         * @param groupByChanel the group by chanel
         * @param groupByTypes the group by types
         * @param aggChannels the agg channels
         * @param aggTypes the agg types
         * @param aggFunctionTypes the agg function types
         * @param aggOutputTypes the agg output types
         * @param inputRaw the input raw
         * @param outputPartial the output partial
         */
        public JitContext(String[] groupByChanel, VecType[] groupByTypes, String[] aggChannels, VecType[] aggTypes,
                FunctionType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw, boolean outputPartial) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.groupByTypes = requireNonNull(groupByTypes, "groupByTypes");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(groupByTypes),
                    Arrays.hashCode(aggChannels), Arrays.hashCode(aggTypes), Arrays.hashCode(aggFunctionTypes),
                    inputRaw, outputPartial);
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
            return Arrays.equals(groupByChanel, that.groupByChanel) && Arrays.equals(groupByTypes, that.groupByTypes)
                    && Arrays.equals(aggTypes, that.aggTypes) && Arrays.equals(aggChannels, that.aggChannels)
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
            return createHashAggregationJitContext(context.groupByChanel,
                    VecTypeSerializer.serialize(context.groupByTypes), context.aggChannels,
                    VecTypeSerializer.serialize(context.aggTypes), toNativeConstants(context.aggFunctionTypes),
                    VecTypeSerializer.serialize(context.aggOutputTypes), context.inputRaw, context.outputPartial);
        }
    }
}
