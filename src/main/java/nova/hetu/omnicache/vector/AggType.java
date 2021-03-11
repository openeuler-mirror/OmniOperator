package nova.hetu.omnicache.vector;

public enum AggType
{
    SUM(0);
    private int value;

    private AggType(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }
}
