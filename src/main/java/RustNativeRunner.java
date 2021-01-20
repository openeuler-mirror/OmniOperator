public class RustNativeRunner
{
    // This declares that the static `hello` method will be provided
    // a native library.
    private static native String allocate(String size);

    static {
        // This actually loads the shared object that we'll be creating.
        // The actual location of the .so or .dll may differ based on your
        // platform.
        System.out.println(System.getProperty("java.library.path"));

        System.loadLibrary("vector");
    }

    // The rest is just regular ol' Java!
    public static void main(String[] args) {
        String output = RustNativeRunner.allocate("10");
        System.out.println(output);
    }
}
