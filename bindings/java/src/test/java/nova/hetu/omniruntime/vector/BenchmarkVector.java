
package nova.hetu.omniruntime.vector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
import java.nio.LongBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
/**
 * benchmar vector
 */
public class BenchmarkVector {
    private static final int ROWS = 8192;
    private static final int GET_LOOP_COUNT = 1000;
    private static final int SET_LOOP_COUNT = 1000;

    interface AffinityExecutorFunction {
        void executor(BenchmarkData data);
    }

    private void affinityExecutor(AffinityExecutorFunction fn, BenchmarkData data) {
        if (data.cpuAffinity) {
            try (AffinityLock lock = AffinityLock.acquireLock(data.cpuUsed.getAndIncrement() % 16)) {
                fn.executor(data);
            }
        } else {
            fn.executor(data);
        }
    }

    private void testOmniVecSet(BenchmarkData data) {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            LongVec omnivec = benchmarkData.longVec;
            for (int loopIdx = 0; loopIdx < SET_LOOP_COUNT; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    omnivec.set(idx, sampleData[idx]);
                }
            }
        }, data);
    }

    private void testHeapVecSet(BenchmarkData data) {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            long[] heapvec = benchmarkData.longValues;
            for (int loopIdx = 0; loopIdx < SET_LOOP_COUNT; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    heapvec[idx] = sampleData[idx];
                }
            }
        }, data);
    }

    private void testOmniVecSetDirectcopy(BenchmarkData data) {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            LongVec omnivec = benchmarkData.longVec;
            LongBuffer longBuf = JvmUtils.directBuffer(omnivec.getValuesBuf()).asLongBuffer();
            for (int loopIdx = 0; loopIdx < SET_LOOP_COUNT; loopIdx++) {
                longBuf.put(sampleData, 0, sampleData.length);
            }
        }, data);
    }

    private void testHeapVecSetSyscopy(BenchmarkData data) {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            long[] heapvec = benchmarkData.longValues;
            for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
                System.arraycopy(sampleData, 0, heapvec, 0, heapvec.length);
            }
        }, data);
    }

    private void testDirectBufferSetBenchmark(BenchmarkData data) {
        affinityExecutor(benchmarkData -> {
            long[] sampleData = benchmarkData.sampleData;
            ByteBuffer byteBuffer = benchmarkData.getLongDirectVec();
            for (int loopIdx = 0; loopIdx < SET_LOOP_COUNT; loopIdx++) {
                for (int idx = 0; idx < ROWS; idx++) {
                    byteBuffer.putLong(idx, sampleData[idx]);
                }
            }
        }, data);
    }

    /**
     * createLongVec
     */
    @Benchmark
    public void createLongVec(BenchmarkData benchmarkData) {
        long[] values = benchmarkData.getLongValues();
        LongVec longVec = new LongVec(values.length);
        for (int i = 0; i < values.length; i++) {
            longVec.set(i, values[i]);
        }
        longVec.close();
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testHeapVecSet032(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testOmnivecSet032(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecSet064(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testOmnivecSet064(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testHeapvecSet128(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testOmnivecSet128(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testHeapvecGet032(BenchmarkData benchmarkData) {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testOmnivecGet032(BenchmarkData benchmarkData) {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecGet064(BenchmarkData benchmarkData) {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testOmnivecGet064(BenchmarkData benchmarkData) {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testHeapvecGet128(BenchmarkData benchmarkData) {
        long[] heapvec = benchmarkData.longValues;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = heapvec[idx];
            }
        }
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testOmnivecGet128(BenchmarkData benchmarkData) {
        LongVec omnivec = benchmarkData.longVec;
        for (int loopIdx = 0; loopIdx < GET_LOOP_COUNT; loopIdx++) {
            for (int idx = 0; idx < ROWS; idx++) {
                long value = omnivec.get(idx);
            }
        }
    }

    /**
     * benchmark 2 threads
     */
    @Benchmark
    @Threads(2)
    public void testHeapvecSet002(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 2 threads
     */
    @Benchmark
    @Threads(2)
    public void testOmnivecSet002(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 2 threads
     */
    @Benchmark
    @Threads(2)
    public void testOmnivecSetDirectcopy002(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 2 threads
     */
    @Benchmark
    @Threads(2)
    public void testHeapvecSetSyscopy002(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 4 threads
     */
    @Benchmark
    @Threads(4)
    public void testHeapvecSet004(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 4 threads
     */
    @Benchmark
    @Threads(4)
    public void testOmnivecSet004(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 4 threads
     */
    @Benchmark
    @Threads(4)
    public void testOmnivecSetDirectcopy004(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 4 threads
     */
    @Benchmark
    @Threads(4)
    public void testHeapvecSetSyscopy004(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 8 threads
     */
    @Benchmark
    @Threads(8)
    public void testHeapvecSet008(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 8 threads
     */
    @Benchmark
    @Threads(8)
    public void testOmnivecSet008(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 8 threads
     */
    @Benchmark
    @Threads(8)
    public void testOmnivecSetDirectcopy008(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 8 threads
     */
    @Benchmark
    @Threads(8)
    public void testHeapvecSetSyscopy008(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 16 threads
     */
    @Benchmark
    @Threads(16)
    public void testHeapvecSet016(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 16 threads
     */
    @Benchmark
    @Threads(16)
    public void testOmnivecSet016(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 16 threads
     */
    @Benchmark
    @Threads(16)
    public void testOmnivecSetDirectcopy016(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 16 threads
     */
    @Benchmark
    @Threads(16)
    public void testHeapvecSetSyscopy016(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testHeapvecSet001(BenchmarkData benchmarkData) {
        testHeapVecSet(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testOmnivecSet001(BenchmarkData benchmarkData) {
        testOmniVecSet(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testOmnivecSetDirectcopy001(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testHeapvecSetSyscopy001(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testHeapvecSetSyscopy032(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecSetSyscopy064(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testHeapvecSetSyscopy128(BenchmarkData benchmarkData) {
        testHeapVecSetSyscopy(benchmarkData);
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testOmnivecSetDirectcopy032(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testOmnivecSetDirectcopy064(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testOmnivecSetDirectcopy128(BenchmarkData benchmarkData) {
        testOmniVecSetDirectcopy(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testDirectbufferSet001(BenchmarkData benchmarkData) {
        testDirectBufferSetBenchmark(benchmarkData);
    }

    /**
     * benchmark 1 threads
     */
    @Benchmark
    @Threads(1)
    public void testHeapvecFilterCopy001(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 2 threads
     */
    @Benchmark
    @Threads(2)
    public void testHeapvecFilterCopy002(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 4 threads
     */
    @Benchmark
    @Threads(4)
    public void testHeapvecFilterCopy004(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 8 threads
     */
    @Benchmark
    @Threads(8)
    public void testHeapvecFilterCopy008(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 16 threads
     */
    @Benchmark
    @Threads(16)
    public void testHeapvecFilterCopy016(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 32 threads
     */
    @Benchmark
    @Threads(32)
    public void testHeapvecFilterCopy032(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 64 threads
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecFilterCopy064(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * benchmark 128 threads
     */
    @Benchmark
    @Threads(128)
    public void testHeapvecFilterCopy128(BenchmarkData benchmarkData) {
        testHeapCopy(benchmarkData);
    }

    /**
     * createLongVecDirect
     */
    @Benchmark
    public void createLongVecDirect(BenchmarkData benchmarkData) {
        long[] values = benchmarkData.getLongValues();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8 * values.length);
        for (long value : values) {
            byteBuffer.putLong(8 * value);
        }
    }

    /**
     * getLongValues
     */
    @Benchmark
    public void getLongValues(BenchmarkData benchmarkData) {
        LongVec longVec = benchmarkData.getLongVec();
        for (int i = 0; i < longVec.getSize(); i++) {
            longVec.get(i);
        }
    }

    /**
     * getDirectLongValues
     */
    @Benchmark
    public void getDirectLongValues(BenchmarkData benchmarkData) {
        ByteBuffer byteBuffer = benchmarkData.getLongDirectVec();
        for (int i = 0; i < byteBuffer.capacity() / 8; i++) {
            byteBuffer.getLong(8 * i);
        }
    }

    /**
     * testHeapCopy
     */
    @Benchmark
    public void testHeapCopy(BenchmarkData benchmarkData) {
        int[] selectedPositions = benchmarkData.selectedPositions;
        long[] originalVec = benchmarkData.heapVecFilterCopyData;

        for (int i = 0; i < 10; i++) {
            long[] newData = new long[selectedPositions.length];
            for (int j = 0; j < selectedPositions.length; j++) {
                newData[j] = originalVec[selectedPositions[j]];
            }
        }
    }

    /**
     * BenchmarkData
     */
    @State(Scope.Benchmark)
    public static class BenchmarkData {
        private final Random random = new Random(0);
        long[] longValues;
        LongVec longVec;
        ByteBuffer byteBuffer;
        long[] sampleData;
        boolean cpuAffinity;
        AtomicInteger cpuUsed = new AtomicInteger(0);
        AtomicLong totalUsedTime = new AtomicLong(0);

        long[] heapVecFilterCopyData;
        LongVec omniVecFilterCopyData;
        int[] selectedPositions;

        public BenchmarkData() {
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

            int rowNum = 3000;
            selectedPositions = new int[rowNum / 2];
            for (int i = 0; i < rowNum / 2; i++) {
                selectedPositions[i] = 2 * i;
            }

            heapVecFilterCopyData = new long[rowNum];
            for (int i = 0; i < rowNum; i++) {
                heapVecFilterCopyData[i] = i;
            }

            omniVecFilterCopyData = new LongVec(rowNum);
            for (int i = 0; i < rowNum; i++) {
                omniVecFilterCopyData.set(i, i);
            }
        }

        public long[] getLongValues() {
            return longValues;
        }

        public LongVec getLongVec() {
            return longVec;
        }

        /**
         * getLongDirectVec
         */
        public ByteBuffer getLongDirectVec() {
            return byteBuffer;
        }
    }

    public static void main(String[] args) throws Throwable {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkVector.class.getSimpleName() + ".test_.*_filter_copy.*")
                .jvmArgs("-Xms2g", "-Xmx16g", "-XX:MaxDirectMemorySize=16g").build();

        new Runner(options).run();
    }
}
