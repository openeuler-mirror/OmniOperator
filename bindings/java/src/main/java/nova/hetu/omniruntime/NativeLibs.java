package nova.hetu.omniruntime;

public class NativeLibs
{
    private static final String OMNI_RUNTIME = "omruntime";

    private NativeLibs()
    {
    }

    public static void load()
    {
        System.loadLibrary(OMNI_RUNTIME);
    }
}
