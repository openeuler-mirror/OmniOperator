package nova.hetu.omnicache.vector;

import net.openhft.affinity.AffinityLock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 50, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkVector
{
    public static final int ROWS = 8192;
    private static final int getLoopCount = 1000;
    private static final int setLoopCount = 1000;

    interface AffinityExecutorFunction
    {
        void executor(BenchmarkData data);
    }

    private void affinityExecutor(AffinityExecutorFunction fn, BenchmarkData data)
    {
        if (data.cpuAffinity) {
            try (AffinityLock lock = AffinityLock.acquireLock(data.cpuUsed.getAndIncrement() % 16)) {
                fn.executor(data);
            }
        }
        else {
            fn.executor(data);
        }
    }

    private void test_omnivec_set(BenchmarkData data)
    {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            LongVec omnivec = benchmarkData.longVec;
            for (int loopIdx = 0; loopIdx < setLoopCount; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    omnivec.set(idx, sampleData[idx]);
                }
            }
        }, data);
    }

    private void test_heapvec_set(BenchmarkData data)
    {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            long[] heapvec = benchmarkData.longValues;
            for (int loopIdx = 0; loopIdx < setLoopCount; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    heapvec[idx] = sampleData[idx];
                }
            }
        }, data);
    }

    private void test_omnivec_set_directcopy(BenchmarkData data)
    {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            LongVec omnivec = benchmarkData.longVec;
            for (int loopIdx = 0; loopIdx < setLoopCount; loopIdx++) {
                omnivec.getData().asLongBuffer().put(sampleData, 0, sampleData.length);
            }
        }, data);
    }

    private void test_heapvec_set_syscopy(BenchmarkData data)
    {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            long[] heapvec = benchmarkData.longValues;
            for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
                System.arraycopy(sampleData, 0, heapvec, 0, heapvec.length);
            }
        }, data);
    }

    private void test_direct_buffer_set_Benchmark(BenchmarkData data)
    {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            ByteBuffer byteBuffer = benchmarkData.getLongDirectVec();
            for (int loopIdx = 0; loopIdx < setLoopCount; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    byteBuffer.putLong(idx, sampleData[idx]);
                }
            }
        }, data);
    }

    @Benchmark
    public void createLongVec(BenchmarkData benchmarkData)
    {
        long[] values = benchmarkData.getLongValues();
        LongVec longVec = new LongVec(values.length);
        for (int i = 0; i < values.length; i++) {
            longVec.set(i, values[i]);
        }
        longVec.close();
    }

    @Benchmark
    @Threads(32)
    public void test_heapvec_set_032(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(32)
    public void test_omnivec_set_032(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(64)
    public void test_heapvec_set_064(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(64)
    public void test_omnivec_set_064(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(128)
    public void test_heapvec_set_128(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(128)
    public void test_omnivec_set_128(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(32)
    public void test_heapvec_get_032(BenchmarkData benchmarkData)
    {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    @Benchmark
    @Threads(32)
    public void test_omnivec_get_032(BenchmarkData benchmarkData)
    {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    @Benchmark
    @Threads(64)
    public void test_heapvec_get_064(BenchmarkData benchmarkData)
    {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    @Benchmark
    @Threads(64)
    public void test_omnivec_get_064(BenchmarkData benchmarkData)
    {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    @Benchmark
    @Threads(128)
    public void test_heapvec_get_128(BenchmarkData benchmarkData)
    {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    @Benchmark
    @Threads(128)
    public void test_omnivec_get_128(BenchmarkData benchmarkData)
    {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < getLoopCount; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    @Benchmark
    @Threads(2)
    public void test_heapvec_set_002(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(2)
    public void test_omnivec_set_002(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(2)
    public void test_omnivec_set_directcopy_002(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(2)
    public void test_heapvec_set_syscopy_002(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(4)
    public void test_heapvec_set_004(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(4)
    public void test_omnivec_set_004(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(4)
    public void test_omnivec_set_directcopy_004(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(4)
    public void test_heapvec_set_syscopy_004(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(8)
    public void test_heapvec_set_008(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(8)
    public void test_omnivec_set_008(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(8)
    public void test_omnivec_set_directcopy_008(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(8)
    public void test_heapvec_set_syscopy_008(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(16)
    public void test_heapvec_set_016(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(16)
    public void test_omnivec_set_016(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(16)
    public void test_omnivec_set_directcopy_016(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(16)
    public void test_heapvec_set_syscopy_016(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(1)
    public void test_heapvec_set_001(BenchmarkData benchmarkData)
    {
        test_heapvec_set(benchmarkData);
    }

    @Benchmark
    @Threads(1)
    public void test_omnivec_set_001(BenchmarkData benchmarkData)
    {
        test_omnivec_set(benchmarkData);
    }

    @Benchmark
    @Threads(1)
    public void test_omnivec_set_directcopy_001(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(1)
    public void test_heapvec_set_syscopy_001(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(32)
    public void test_heapvec_set_syscopy_032(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(64)
    public void test_heapvec_set_syscopy_064(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(128)
    public void test_heapvec_set_syscopy_128(BenchmarkData benchmarkData)
    {
        test_heapvec_set_syscopy(benchmarkData);
    }

    @Benchmark
    @Threads(32)
    public void test_omnivec_set_directcopy_032(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(64)
    public void test_omnivec_set_directcopy_064(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(128)
    public void test_omnivec_set_directcopy_128(BenchmarkData benchmarkData)
    {
        test_omnivec_set_directcopy(benchmarkData);
    }

    @Benchmark
    @Threads(1)
    public void test_directbuffer_set_001(BenchmarkData benchmarkData)
    {
        test_direct_buffer_set_Benchmark(benchmarkData);
    }

    @Benchmark
    public void createLongVecDirect(BenchmarkData benchmarkData)
    {
        long[] values = benchmarkData.getLongValues();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8 * values.length);
        for (long value : values) {
            byteBuffer.putLong(8 * value);
        }
    }

    @Benchmark
    public void getLongValues(BenchmarkData benchmarkData)
    {
        LongVec longVec = benchmarkData.getLongVec();
        for (int i = 0; i < longVec.size(); i++) {
            longVec.get(i);
        }
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
        long[] sampleData;
        boolean cpuAffinity = false;
        AtomicInteger cpuUsed = new AtomicInteger(0);
        AtomicLong totalUsedTime = new AtomicLong(0);

        public BenchmarkData()
        {
            longValues = new long[ROWS];
            for (int i = 0; i < longValues.length; i++) {
                longValues[i] = random.nextLong();
            }
            sampleData = new long[ROWS];
            for (int i = 0; i < sampleData.length; i++) {
                sampleData[i] = random.nextLong();
            }

            longVec = new LongVec(longValues.length);
            for (int i = 0; i < longValues.length; i++) {
                longVec.set(i, longValues[i]);
            }
            byteBuffer = ByteBuffer.allocateDirect(8 * longValues.length);
            for (long longValue : longValues) {
                byteBuffer.putLong(longValue);
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
                .jvmArgs("-Xms2g", "-Xmx16g", "-XX:MaxDirectMemorySize=16g")
                .build();

        new Runner(options).run();
    }
}
