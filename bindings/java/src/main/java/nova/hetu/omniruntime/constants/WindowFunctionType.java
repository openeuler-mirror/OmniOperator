package nova.hetu.omniruntime.constants;

@SuppressWarnings("StaticVariableName")
public class WindowFunctionType
        extends Constant
{
    public static WindowFunctionType WIN_ROW_NUMBER = new WindowFunctionType(0);
    public static WindowFunctionType WIN_RANK = new WindowFunctionType(1);
    public static WindowFunctionType WIN_SUM = new WindowFunctionType(2);
    public static WindowFunctionType WIN_COUNT = new WindowFunctionType(3);
    public static WindowFunctionType WIN_AVG = new WindowFunctionType(4);
    public static WindowFunctionType WIN_MAX = new WindowFunctionType(5);
    public static WindowFunctionType WIN_MIN = new WindowFunctionType(6);

    public WindowFunctionType(int value)
    {
        super(value);
    }
}
