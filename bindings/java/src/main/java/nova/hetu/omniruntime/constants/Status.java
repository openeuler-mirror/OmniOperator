package nova.hetu.omniruntime.constants;

@SuppressWarnings("StaticVariableName")
public class Status
        extends Constant
{
    public static Status OMNI_STATUS_NORMAL;

    public static Status OMNI_STATUS_ERROR;

    public static Status OMNI_STATUS_FINISHED;

    public Status(int value)
    {
        super(value);
    }
}
