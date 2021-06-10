package nova.hetu.omniruntime.utils;

public enum OmniErrorType
{
    OMNI_UNDEFINED(1),
    OMNI_NOSUPPORT(2),
    OMNI_NATIVE_ERROR(3);

    private int value;
    OmniErrorType(int v)
    {
        this.value = v;
    }

    public int getValue()
    {
        return value;
    }
}
