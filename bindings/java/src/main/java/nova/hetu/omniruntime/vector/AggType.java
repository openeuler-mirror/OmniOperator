package nova.hetu.omniruntime.vector;

public enum AggType
{
    SUM(0),
    COUNT(1),
    AVG(2),
    MAX(3),
    MIN(4),
    DISTINCT(5);

    private int value;

    AggType(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }
}
