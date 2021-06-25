package nova.hetu.omniruntime.constants;

@SuppressWarnings("StaticVariableName")
public class AggType
        extends Constant
{
    public static AggType OMNI_AGGREGATION_TYPE_SUM;

    public static AggType OMNI_AGGREGATION_TYPE_COUNT;

    public static AggType OMNI_AGGREGATION_TYPE_AVG;

    public static AggType OMNI_AGGREGATION_TYPE_MAX;

    public static AggType OMNI_AGGREGATION_TYPE_MIN;

    public static AggType OMNI_AGGREGATION_TYPE_DNV;

    public AggType(int value)
    {
        super(value);
    }
}
