package nova.hetu.omnicache.vector;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkVector.ROWS)
public class BenchmarkVector
{
    public static final int ROWS = 10000;

    @Benchmark
    public Vec createLongVec(BenchmarkData benchmarkData)
    {
        long[] values = benchmarkData.getLongValues();
        LongVec longVec = new LongVec(values.length);
        for (int i = 0; i < values.length; i++) {
            longVec.set(i, values[i]);
        }
        return longVec;
    }

    @Benchmark
    public ByteBuffer createLongVecDirect(BenchmarkData benchmarkData)
    {
        long[] values = benchmarkData.getLongValues();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8 * values.length);
        for (int i = 0; i < values.length; i++) {
            byteBuffer.putLong(8 * values[i]);
        }
        return byteBuffer;
    }

    @Benchmark
    public void getLongValues(BenchmarkData benchmarkData)
    {
        LongVec longVec = benchmarkData.getLongVec();
        for (int i = 0; i < longVec.size(); i++) {
            longVec.get(i);
        }
//        ByteBuffer longVec = benchmarkData.getLongVec().getData();
//        for (int i = 0; i < longVec.capacity()/8; i++) {
//            longVec.getLong(i*8);
//        }
//        DirectLongVec longVec = benchmarkData.getDirectLongVec();
//        for (int i = 0; i < longVec.size(); i++) {
//            longVec.get(i);
//        }
    }

    @Benchmark
    public void getDirectLongValues(BenchmarkData benchmarkData)
    {
        ByteBuffer byteBuffer = benchmarkData.getLongDirectVec();
        for (int i = 0; i < byteBuffer.capacity() / 8; i++) {
            byteBuffer.getLong(8 * i);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        protected final Random random = new Random(0);
        long[] longValues;
        LongVec longVec;
        ByteBuffer byteBuffer;
        DirectLongVec directLongVec;

        public BenchmarkData()
        {
            longValues = new long[ROWS];
            for (int i = 0; i < longValues.length; i++) {
                longValues[i] = random.nextLong();
            }
            longVec = new LongVec(longValues.length);
            for (int i = 0; i < longValues.length; i++) {
                longVec.set(i, longValues[i]);
            }
            byteBuffer = ByteBuffer.allocateDirect(8 * longValues.length);
            for (int i = 0; i < longValues.length; i++) {
                byteBuffer.putLong(longValues[i]);
            }
            directLongVec = new DirectLongVec(longValues.length);
            for (int i = 0; i < longValues.length; i++) {
                directLongVec.set(i, longValues[i]);
            }
        }

        public long[] getLongValues()
        {
            return longValues;
        }

        public LongVec getLongVec()
        {
            return longVec;
        }

        public DirectLongVec getDirectLongVec()
        {
            return directLongVec;
        }

        public ByteBuffer getLongDirectVec()
        {
            return byteBuffer;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkVector.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
