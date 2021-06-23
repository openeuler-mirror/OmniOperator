package nova.hetu.omniruntime.constants;

import java.util.Arrays;

public class ConstantHelper
{
    private ConstantHelper()
    {
    }

    public static int[] toNativeConstants(Constant[] constants)
    {
        return Arrays.stream(constants).map(Constant::getValue).mapToInt(Integer::intValue).toArray();
    }
}
