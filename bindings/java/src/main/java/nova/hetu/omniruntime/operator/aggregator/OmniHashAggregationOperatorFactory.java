package nova.hetu.omniruntime.operator.aggregator;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.AggType;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniHashAggregationOperatorFactory
        extends OmniOperatorFactory
{
    private int[] groupByChanel;
    private VecType[] groupByTypes;
    private int[] aggChannels;
    private VecType[] aggTypes;
    private AggType[] aggFunctionTypes;
    private VecType[] aggOutputTypes;

    public OmniHashAggregationOperatorFactory(int[] groupByChanel,
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
    protected long createNativeOperatorFactory()
    {
        return createHashAggregationOperatorFactory(groupByChanel,
                OmniUtils.transformVecType(groupByTypes),
                aggChannels,
                OmniUtils.transformVecType(aggTypes),
                OmniUtils.transformAggType(aggFunctionTypes),
                OmniUtils.transformVecType(aggOutputTypes));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupByTypes, aggTypes, aggFunctionTypes);
    }

    private static native long createHashAggregationOperatorFactory(int[] groupByChanel, int[] groupByTypes, int[] aggChannels, int[] aggTypes, int[] aggFunctionTypes, int[] aggOutputTypes);
}
