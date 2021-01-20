package nova.hetu.omnicache.benchmark;
/**
 * Run with this command to show native assembly:<br/>
 * Java -XX:+UnlockDiagnosticVMOptions
 * -XX:CompileCommand=print,VectorizationMicroBenchmark.square
 * VectorizationMicroBenchmark
 */
public class VectorizationMicroBenchmark {

    private static void square(float[] a) {
        for (int i = 0; i < a.length; i++) {
            a[i] = a[i] * a[i]; // line 11
        }
    }

    public static void main(String[] args) throws Exception {
        float[] a = new float[1024];

        // repeatedly invoke the method under test. this
        // causes the JIT compiler to optimize the method
        for (int i = 0; i < 1000 * 1000; i++) {
            square(a);
        }
    }
}