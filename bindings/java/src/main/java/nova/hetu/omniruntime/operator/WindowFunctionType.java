package nova.hetu.omniruntime.operator;

public enum WindowFunctionType
{
    ROW_NUMBER(0),
    RANK(1);

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
