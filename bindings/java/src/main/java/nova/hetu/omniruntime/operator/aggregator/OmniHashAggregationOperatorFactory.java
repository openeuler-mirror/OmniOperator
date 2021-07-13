/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.aggregator;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni hash aggregation operator factory.
 *
 * @since 20210630
 */
public class OmniHashAggregationOperatorFactory
    extends OmniOperatorFactory<OmniHashAggregationOperatorFactory.Context> {
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
    public OmniHashAggregationOperatorFactory(int[] groupByChanel, VecType[] groupByTypes, int[] aggChannels,
        VecType[] aggTypes, AggType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw,
        boolean outputPartial) {
        super(
            new Context(groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes, inputRaw,
                outputPartial));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createHashAggregationOperatorFactory(context.groupByChanel,
                toNativeConstants(context.groupByTypes),
                context.aggChannels,
                toNativeConstants(context.aggTypes),
                toNativeConstants(context.aggFunctionTypes),
                toNativeConstants(context.aggOutputTypes),
                context.inputRaw,
                context.outputPartial);
    }

    private static native long createHashAggregationOperatorFactory(int[] groupByChanel,
            int[] groupByTypes,
            int[] aggChannels,
            int[] aggTypes,
            int[] aggFunctionTypes,
            int[] aggOutputTypes,
            boolean inputRaw,
            boolean outputPartial);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final int[] groupByChanel;
        private final VecType[] groupByTypes;
        private final int[] aggChannels;
        private final VecType[] aggTypes;
        private final AggType[] aggFunctionTypes;
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
        public Context(int[] groupByChanel, VecType[] groupByTypes, int[] aggChannels, VecType[] aggTypes,
            AggType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw, boolean outputPartial) {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.groupByTypes = requireNonNull(groupByTypes, "groupByTypes");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.inputRaw = requireNonNull(inputRaw, "inputRaw");
            this.outputPartial = requireNonNull(outputPartial, "outputPartial");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(groupByChanel), Arrays.hashCode(groupByTypes), Arrays.hashCode(aggChannels), Arrays.hashCode(aggTypes), Arrays.hashCode(aggFunctionTypes));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Context that = (Context) obj;
            return Arrays.equals(groupByChanel, that.groupByChanel) && Arrays.equals(groupByTypes, that.groupByTypes)
                && Arrays.equals(aggTypes, that.aggTypes) && Arrays.equals(aggChannels, that.aggChannels)
                && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes);
        }
    }
}
