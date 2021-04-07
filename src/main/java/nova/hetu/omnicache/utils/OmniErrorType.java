package nova.hetu.omnicache.utils;

public enum OmniErrorType
{
    OMNI_UNDIFINED(1),
    OMNI_NOSUPPORT(2);
    private int value;
    OmniErrorType(int v){
        this.value = v;
    }

    public int getValue()
    {
        return value;
    }
}
