package nova.hetu.omniruntime.operator.aggregator;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.AggType;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniAggregationOperatorFactory
            extends OmniOperatorFactory<OmniAggregationOperatorFactory.Context>
{
    public OmniAggregationOperatorFactory(VecType[] aggTypes,
                                          AggType[] aggFunctionTypes,
                                          VecType[] aggOutputTypes)
    {
        super(new Context(aggTypes, aggFunctionTypes, aggOutputTypes));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createAggregationOperatorFactory(
                OmniUtils.transformVecType(context.aggTypes),
                OmniUtils.transformAggType(context.aggFunctionTypes),
                OmniUtils.transformVecType(context.aggOutputTypes));
    }

    private static native long createAggregationOperatorFactory(int[] aggTypes, int[] aggFunctionTypes, int[] aggOutputTypes);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] aggTypes;
        private final AggType[] aggFunctionTypes;
        private final VecType[] aggOutputTypes;

        public Context(VecType[] aggTypes,
                       AggType[] aggFunctionTypes,
                       VecType[] aggOutputTypes)
        {
            this.aggTypes = requireNonNull(aggTypes, "aggTypes");
            this.aggFunctionTypes = requireNonNull(aggFunctionTypes, "aggFunctionTypes");
            this.aggOutputTypes = requireNonNull(aggOutputTypes, "aggOutputTypes");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(aggTypes, aggFunctionTypes);
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
            return Objects.equals(aggTypes, that.aggTypes)
                    && Objects.equals(aggFunctionTypes, that.aggFunctionTypes);
        }
    }
}
