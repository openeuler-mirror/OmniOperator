package nova.hetu.omniruntime.utils;

import nova.hetu.omniruntime.constants.VecType;

public class VectorHelper
{
    private VectorHelper()
    {
    }

    public static VecType[] transformIntToVecType(int[] values)
    {
        VecType[] vecTypes = new VecType[values.length];
        for (int i = 0; i < values.length; ++i) {
            vecTypes[i] = new VecType(values[i]);
        }
        return vecTypes;
    }
}
