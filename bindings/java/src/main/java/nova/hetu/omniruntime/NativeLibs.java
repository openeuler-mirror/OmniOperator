package nova.hetu.omniruntime;

public class NativeLibs
{
    private final static String OMNI_RUNTIME = "omruntime";

    public static void load()
    {
        System.loadLibrary(OMNI_RUNTIME);
    }
}
