package nova.hetu.omniruntime.constants;

@SuppressWarnings("StaticVariableName")
public class VecType
        extends Constant
{
    public static VecType OMNI_VEC_TYPE_INT;

    public static VecType OMNI_VEC_TYPE_LONG;

    public static VecType OMNI_VEC_TYPE_DOUBLE;

    public static VecType OMNI_VEC_TYPE_BOOLEAN;

    public static VecType OMNI_VEC_TYPE_SHORT;

    public static VecType OMNI_VEC_TYPE_128_DECIMAL;

    public static VecType OMNI_VEC_TYPE_256_DECIMAL;

    public static VecType OMNI_VEC_TYPE_VARCHAR;

    public static VecType OMNI_VEC_TYPE_CONTAINER;

    public VecType(int value)
    {
        super(value);
    }
}
