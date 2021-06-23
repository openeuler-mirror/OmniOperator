package nova.hetu.omniruntime;

public class OmniLibs
{
    private static final String OMNI_RUNTIME = "omruntime";

    private OmniLibs()
    {
    }

    public static void load()
    {
        System.loadLibrary(OMNI_RUNTIME);
    }
}
