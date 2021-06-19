package nova.hetu.omniruntime.operator;

public enum WindowFunctionType
{
    WIN_ROW_NUMBER(0),
    WIN_RANK(1),
    WIN_SUM(2),
    WIN_COUNT(3),
    WIN_AVG(4),
    WIN_MAX(5),
    WIN_MIN(6);

    private int value;

    WindowFunctionType(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }
}
