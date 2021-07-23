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
 * The type Omni aggregation operator factory.
 *
 * @since 20210630
 */
public class OmniAggregationOperatorFactory extends OmniOperatorFactory<OmniAggregationOperatorFactory.Context> {
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
        super(new Context(aggTypes, aggFunctionTypes, aggOutputTypes, inputRaw, outputPartial));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createAggregationOperatorFactory(
                toNativeConstants(context.aggTypes),
                toNativeConstants(context.aggFunctionTypes),
                toNativeConstants(context.aggOutputTypes),
                context.inputRaw,
                context.outputPartial);
    }

    private static native long createAggregationOperatorFactory(int[] aggTypes, int[] aggFunctionTypes, int[] aggOutputTypes, boolean inputRaw, boolean outputPartial);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
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
        public Context(VecType[] aggTypes, AggType[] aggFunctionTypes, VecType[] aggOutputTypes, boolean inputRaw,
            boolean outputPartial) {
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
            this.inputRaw = requireNonNull(inputRaw, "inputRaw");
            this.outputPartial = requireNonNull(outputPartial, "outputPartial");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(aggTypes), Arrays.hashCode(aggFunctionTypes));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OmniAggregationOperatorFactory.Context that = (OmniAggregationOperatorFactory.Context) o;
            return Arrays.equals(aggTypes, that.aggTypes)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes);
        }
    }
}
