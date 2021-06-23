package nova.hetu.omniruntime.operator.aggregator;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

public class OmniHashAggregationOperatorFactory
        extends OmniOperatorFactory<OmniHashAggregationOperatorFactory.Context>
{
    public OmniHashAggregationOperatorFactory(int[] groupByChanel,
            VecType[] groupByTypes,
            int[] aggChannels,
            VecType[] aggTypes,
            AggType[] aggFunctionTypes,
            VecType[] aggOutputTypes)
    {
        super(new Context(groupByChanel, groupByTypes, aggChannels, aggTypes, aggFunctionTypes, aggOutputTypes));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createHashAggregationOperatorFactory(context.groupByChanel,
                toNativeConstants(context.groupByTypes),
                context.aggChannels,
                toNativeConstants(context.aggTypes),
                toNativeConstants(context.aggFunctionTypes),
                toNativeConstants(context.aggOutputTypes));
    }

    private static native long createHashAggregationOperatorFactory(int[] groupByChanel, int[] groupByTypes, int[] aggChannels, int[] aggTypes, int[] aggFunctionTypes, int[] aggOutputTypes);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final int[] groupByChanel;
        private final VecType[] groupByTypes;
        private final int[] aggChannels;
        private final VecType[] aggTypes;
        private final AggType[] aggFunctionTypes;
        private final VecType[] aggOutputTypes;

        public Context(int[] groupByChanel,
                VecType[] groupByTypes,
                int[] aggChannels,
                VecType[] aggTypes,
                AggType[] aggFunctionTypes,
                VecType[] aggOutputTypes)
        {
            this.groupByChanel = requireNonNull(groupByChanel, "requireNonNull");
            this.groupByTypes = requireNonNull(groupByTypes, "groupByTypes");
            this.aggChannels = requireNonNull(aggChannels, "aggChannels");
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(groupByTypes), Arrays.hashCode(aggTypes), Arrays.hashCode(aggFunctionTypes));
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
            Context that = (Context) o;
            return Arrays.equals(groupByTypes, that.groupByTypes)
                    && Arrays.equals(aggTypes, that.aggTypes)
                    && Arrays.equals(aggFunctionTypes, that.aggFunctionTypes);
        }
    }
}
