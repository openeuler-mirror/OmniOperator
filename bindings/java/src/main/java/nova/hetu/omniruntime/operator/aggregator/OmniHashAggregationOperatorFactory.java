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
 * The type Omni hash aggregation operator factory.
 *
 * @since 2021-06-30
 */
public class OmniHashAggregationOperatorFactory
        extends OmniOperatorFactory<OmniHashAggregationOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash aggregation operator factory.
     *
     * @param groupByChanel the group by chanel
     * @param groupByTypes the group by types
     * @param aggChannels the agg channels
     * @param aggTypes the agg types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param isInputRaw the input raw
     * @param isOutputPartial the output partial
     * @param isJitEnabled whether the jit is enabled
     */
    public OmniHashAggregationOperatorFactory(String[] groupByChanel, DataType[] groupByTypes, String[] aggChannels,
            DataType[] aggTypes, FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial, boolean isJitEnabled) {
        super(new FactoryContext(new JitContext(groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes,
                aggOutputTypes, isInputRaw, isOutputPartial), isJitEnabled));
    }

    /**
     * Instantiates a new Omni hash aggregation operator factory with jit
     * default.
     *
     * @param groupByChanel the group by chanel
     * @param groupByTypes the group by types
     * @param aggChannels the agg channels
     * @param aggTypes the agg types
     * @param aggFunctionTypes the agg function types
     * @param aggOutputTypes the agg output types
     * @param isInputRaw the input raw
     * @param isOutputPartial the output partial
     */
    public OmniHashAggregationOperatorFactory(String[] groupByChanel, DataType[] groupByTypes, String[] aggChannels,
            DataType[] aggTypes, FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial) {
        this(groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes, isInputRaw,
                isOutputPartial, true);
    }

    private static native long createHashAggregationOperatorFactory(String[] groupByChanel, String groupByTypes,
            String[] aggChannels, String aggTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial, long jitContext);

    private static native long createHashAggregationJitContext(String[] groupByChanel, String groupByTypes,
            String[] aggChannels, String aggTypes, int[] aggFunctionTypes, String aggOutputTypes, boolean isInputRaw,
            boolean isOutputPartial);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashAggregationOperatorFactory(context.groupByChanel,
                DataTypeSerializer.serialize(context.groupByTypes), context.aggChannels,
                DataTypeSerializer.serialize(context.aggTypes), toNativeConstants(context.aggFunctionTypes),
                DataTypeSerializer.serialize(context.aggOutputTypes), context.isInputRaw, context.isOutputPartial,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext implements OmniJitContext {
        private final String[] groupByChanel;

        private final DataType[] groupByTypes;

        private final String[] aggChannels;

        private final DataType[] aggTypes;

        private final FunctionType[] aggFunctionTypes;

        private final DataType[] aggOutputTypes;

        private final boolean isInputRaw;

        private final boolean isOutputPartial;

        /**
         * Instantiates a new Context.
         *
         * @param groupByChanel the group by chanel
         * @param groupByTypes the group by types
         * @param aggChannels the agg channels
         * @param aggTypes the agg types
         * @param aggFunctionTypes the agg function types
         * @param aggOutputTypes the agg output types
         * @param isInputRaw the input raw
         * @param isOutputPartial the output partial
         */
        public JitContext(String[] groupByChanel, DataType[] groupByTypes, String[] aggChannels, DataType[] aggTypes,
                FunctionType[] aggFunctionTypes, DataType[] aggOutputTypes, boolean isInputRaw,
                boolean isOutputPartial) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.groupByTypes = requireNonNull(groupByTypes, "groupByTypes");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.isInputRaw = isInputRaw;
            this.isOutputPartial = isOutputPartial;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(groupByTypes),
                    Arrays.hashCode(aggChannels), Arrays.hashCode(aggTypes), Arrays.hashCode(aggOutputTypes),
                    Arrays.hashCode(aggFunctionTypes), isInputRaw, isOutputPartial);
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
                    && Arrays.equals(aggTypes, that.aggTypes) && Arrays.equals(aggOutputTypes, that.aggOutputTypes)
                    && Arrays.equals(aggChannels, that.aggChannels)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes) && isInputRaw == that.isInputRaw
                    && isOutputPartial == that.isOutputPartial;
        }
    }

    /**
     * The type Factory context.
     *
     * @since 2021-06-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         * @param isJitEnabled whether the jit is enabled
         */
        public FactoryContext(JitContext jitContext, boolean isJitEnabled) {
            super(jitContext, isJitEnabled);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return createHashAggregationJitContext(context.groupByChanel,
                    DataTypeSerializer.serialize(context.groupByTypes), context.aggChannels,
                    DataTypeSerializer.serialize(context.aggTypes), toNativeConstants(context.aggFunctionTypes),
                    DataTypeSerializer.serialize(context.aggOutputTypes), context.isInputRaw, context.isOutputPartial);
        }
    }
}
