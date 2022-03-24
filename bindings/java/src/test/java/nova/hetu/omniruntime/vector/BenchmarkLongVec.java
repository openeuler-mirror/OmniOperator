/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 1, batchSize = 1)
@Measurement(iterations = 10, batchSize = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongVec {
    private static final int ALLOCATOR_CAPACITY = 1024 * 1024;
    private static final int COUNT = 1000;

    @Param({"1024"})
    private int rows = 1024;

    // longVec set/put
    private LongVec vecPutData;
    private LongVec vecSetData;
    private long[] arraySetData;
    private long[] randomData;

    // longVec get
    private LongVec vecGetData;
    private LongVec vecGetDatas;
    private long[] results;
    private long[] arrayGetData;
    private int[] positions;

    // arrow set
    private RootAllocator allocator1;
    private BigIntVector arrowLongVecSet;

    // arrow get
    RootAllocator allocator2;
    BigIntVector arrowLongVecGet;

    // init
    private LongVec data = new LongVec(rows);

    private final Random random = new Random(0);

    static {
        // this parameter affects arrow get performance
        System.setProperty("arrow.enable_null_check_for_get", "false");
        // this parameter affects arrow set performance
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
    }

    @Setup(Level.Iteration)
    public void init() {
        // for vec set/put
        vecPutData = new LongVec(rows);
        vecSetData = new LongVec(rows);
        arraySetData = new long[rows];
        randomData = new long[rows];
        initValues(randomData, rows);

        // for vec get
        vecGetData = new LongVec(rows);
        initValues(vecGetData, rows);
        vecGetDatas = new LongVec(rows);
        initValues(vecGetDatas, rows);
        results = new long[rows];

        positions = new int[rows / 2];
        for (int i = 0; i < rows / 2; i++) {
            positions[i] = i;
        }

        // arrow set
        allocator1 = new RootAllocator(ALLOCATOR_CAPACITY);
        arrowLongVecSet = new BigIntVector("longVector1", allocator1);
        arrowLongVecSet.allocateNew(rows);
        arrowLongVecSet.setValueCount(rows);

        // arrow get
        allocator2 = new RootAllocator(ALLOCATOR_CAPACITY);
        arrowLongVecGet = new BigIntVector("longVector2", allocator2);
        arrowLongVecGet.allocateNew(rows);
        initValues(arrowLongVecGet, rows);
        arrowLongVecGet.setValueCount(rows);

        arrayGetData = new long[rows];
        initValues(arrayGetData, rows);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        vecPutData.close();
        vecSetData.close();
        vecGetData.close();

        arrowLongVecSet.close();
        allocator1.close();

        arrowLongVecGet.close();
        allocator2.close();
    }

    private void initValues(LongVec vec, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            vec.set(i, random.nextLong());
        }
    }

    private void initValues(long[] array, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            array[i] = random.nextLong();
        }
    }

    private void initValues(BigIntVector arrayVec, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            arrayVec.set(i, random.nextLong());
        }
    }

    @Benchmark
    public void createLongVecBenchmark(Blackhole blackhole) {
        List<LongVec> vecs = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            LongVec vec = new LongVec(rows);
            blackhole.consume(vec);
            vecs.add(vec);
        }
        closeVec(vecs);
    }

    @Benchmark
    public void createArrowVecBenchmark(Blackhole blackhole) {
        List<BigIntVector> vecs = new ArrayList<>();
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        for (int i = 0; i < COUNT; i++) {
            BigIntVector arrowVec = new BigIntVector("key", rootAllocator);
            arrowVec.allocateNew(rows);
            blackhole.consume(arrowVec);
            vecs.add(arrowVec);
        }
        closeArrowVec(vecs);
        rootAllocator.close();
    }

    @Benchmark
    public void createLongArrayBenchmark(Blackhole benchmarkData) {
        for (int i = 0; i < COUNT; i++) {
            benchmarkData.consume(new long[rows]);
        }
    }

    @Benchmark
    public void setLongVecBenchmark() {
        for (int i = 0; i < rows; i++) {
            vecSetData.set(i, randomData[i]);
        }
    }

    @Benchmark
    public void copyArrayBenchmark() {
        long[] data = new long[rows];
        System.arraycopy(randomData, 0, data, 0, rows);
    }

    @Benchmark
    public void newPutReleaseLongVecBenchmark() {
        LongVec vec = new LongVec(rows);
        vec.put(randomData, 0, 0, randomData.length);
        vec.close();
    }

    @Benchmark
    public void putLongVecBenchmark() {
        vecPutData.put(randomData, 0, 0, randomData.length);
    }

    @Benchmark
    public void setLongArrayBenchmark() {
        for (int i = 0; i < rows; i++) {
            arraySetData[i] = randomData[i];
        }
    }

    @Benchmark
    public void setArrowVecBenchmark() {
        for (int i = 0; i < rows; i++) {
            arrowLongVecSet.set(i, randomData[i]);
        }
    }

    @Benchmark
    public long getLongArrayBenchmark() {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            sum += arrayGetData[i];
        }
        return sum;
    }

    @Benchmark
    public long getLongVecBenchmark() {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            sum += vecGetData.get(i);
        }
        return sum;
    }

    @Benchmark
    public long getsLongVecBenchmark() {
        long sum = 0;
        long[] result = vecGetDatas.get(0, rows);
        for (long datum : result) {
            sum += datum;
        }
        return sum;
    }

    @Benchmark
    public long getArrowVecBenchmark() {
        long sum = 0;
        int rows = this.rows;
        for (int i = 0; i < rows; i++) {
            sum += arrowLongVecGet.get(i);
        }
        return sum;
    }

    @Benchmark
    public int sliceLongVecBenchmark() {
        LongVec slice = vecGetData.slice(2, rows / 2);
        return slice.getSize();
    }

    @Benchmark
    public int copyRegionLongVecBenchmark() {
        LongVec copyRegion = vecGetData.copyRegion(2, rows / 2);
        return copyRegion.getSize();
    }

    @Benchmark
    public int copyPositionLongVecBenchmark() {
        LongVec copyPosition = vecGetData.copyPositions(positions, 0, positions.length);
        return copyPosition.getSize();
    }

    private void closeArrowVec(List<BigIntVector> vecs) {
        for (BigIntVector vec : vecs) {
            vec.close();
        }
    }

    private void closeVec(List<LongVec> vecs) {
        for (Vec vec : vecs) {
            vec.close();
        }
    }

    public static void main(String[] args) throws Throwable {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLongVec.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
