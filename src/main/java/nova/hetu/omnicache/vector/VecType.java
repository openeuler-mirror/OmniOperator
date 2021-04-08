package nova.hetu.omnicache.vector;

public enum VecType {
    INT(1),
    LONG(2),
    DOUBLE(3),
    BOOLEAN(4);
    private int value;

    private VecType(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }
}
