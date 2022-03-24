/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nova.hetu.omniruntime.vector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
/**
 * vec benchmar with pool
 */
public class BenchmarkVecWithPool {
    private static final int PAGE_SIZE = 2048;
    private static final int PAGE_COUNT = 2000;

    /**
     * benchmark 16 thread
     */
    @Benchmark
    @Threads(16)
    public void testOmnivecWithInitialAllPagePerformance16() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testOmniVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 32 thread
     */
    @Benchmark
    @Threads(32)
    public void testOmnivecWithInitialAllPagePerformance32() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testOmniVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 64 thread
     */
    @Benchmark
    @Threads(64)
    public void testOmnivecWithInitialAllPagePerformance64() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testOmniVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 128 thread
     */
    @Benchmark
    @Threads(128)
    public void testOmnivecWithInitialAllPagePerformance128() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testOmniVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 16 thread
     */
    @Benchmark
    @Threads(16)
    public void testHeapvecWithInitialAllPagePerformance16() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testHeapVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 32 thread
     */
    @Benchmark
    @Threads(32)
    public void testHeapvecWithInitialAllPagePerformance32() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testHeapVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 64 thread
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecWithInitialAllPagePerformance64() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testHeapVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 128 thread
     */
    @Benchmark
    @Threads(128)
    public void testHeapvecWithInitialAllPagePerformance128() {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.testHeapVecWithInitialAllPage(PAGE_COUNT, PAGE_SIZE, 1);
    }

    /**
     * benchmark 64 thread
     */
    @Benchmark
    @Threads(64)
    public void testOmnivecPerformance64() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            LongVec[] vecs = dataGenerator.builderGroupBy2CSum2CVecPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    /**
     * benchmark 64 thread
     */
    @Benchmark
    @Threads(64)
    public void testHeapvecPerformance64() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            HeapLongVec[] vecs = dataGenerator.builderGroupBy2CSum2CHeapPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    /**
     * benchmark 64 thread
     */
    @Benchmark
    @Threads(64)
    public void testUnsafevecPerformance64() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            UnsafeLongVec[] vecs = dataGenerator.builderGroupBy2CSum2CUnsafevecPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    /**
     * benchmark 32 thread
     */
    @Benchmark
    @Threads(32)
    public void testOmnivecPerformance32() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            LongVec[] vecs = dataGenerator.builderGroupBy2CSum2CVecPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    /**
     * benchmark 32 thread
     */
    @Benchmark
    @Threads(32)
    public void testHeapvecPerformance32() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            HeapLongVec[] vecs = dataGenerator.builderGroupBy2CSum2CHeapPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    /**
     * benchmark 32 thread
     */
    @Benchmark
    @Threads(32)
    public void testUnsafevecPerformance32() {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < PAGE_COUNT; pageIdx++) {
            UnsafeLongVec[] vecs = dataGenerator.builderGroupBy2CSum2CUnsafevecPageLikeQ1(PAGE_SIZE);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].getSize(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkVecWithPool.class.getSimpleName() + ".test_.*_with_initial_all_page*")
                .jvmArgs("-Xms2g", "-Xmx16g", "-XX:MaxDirectMemorySize=16g").build();

        new Runner(options).run();
    }

    /**
     * data generator
     */
    public class DataGenerator {
        private class PageUseOmniVec {
            private LongVec[] data;

            public PageUseOmniVec(LongVec[] data) {
                this.data = data;
            }

            public LongVec[] getData() {
                return this.data;
            }

            /**
             * release memory
             */
            public void release() {
                for (LongVec vec : data) {
                    vec.close();
                }
            }
        }

        private class PageUseHeapVec {
            private HeapLongVec[] data;

            public PageUseHeapVec(HeapLongVec[] data) {
                this.data = data;
            }

            public HeapLongVec[] getData() {
                return this.data;
            }

            /**
             * release memory
             */
            public void release() {
                for (HeapLongVec vec : data) {
                    vec.close();
                }
            }
        }

        /**
         * ExecutorWithHeapPageInitial
         */
        public class ExecutorWithHeapPageInitial {
            private PageUseHeapVec[] pages;
            private int pageCount;
            private int pageSize;

            public ExecutorWithHeapPageInitial(int pageCount, int pageSize) {
                this.pages = new PageUseHeapVec[pageCount];
                for (int pIdx = 0; pIdx < pageCount; pIdx++) {
                    HeapLongVec[] data = builderGroupBy2CSum2CHeapPageLikeQ1(pageSize);
                    this.pages[pIdx] = new PageUseHeapVec(data);
                }
                this.pageCount = pageCount;
                this.pageSize = pageSize;
            }

            /**
             * executor
             */
            public ExecutorWithHeapPageInitial executor(int loopReadCount) {
                // test read, get vec all data
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseHeapVec pageUseOmniVec = this.pages[pageIdx];
                    for (int vecIdx = 0; vecIdx < 4; vecIdx++) {
                        for (int loop = 0; loop < loopReadCount; loop++) {
                            for (int rIdx = 0; rIdx < pageSize; rIdx++) {
                                pageUseOmniVec.getData()[vecIdx].get(rIdx);
                            }
                        }
                    }
                }
                return this;
            }

            /**
             * release vector memory
             */
            public void release() {
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseHeapVec pageUseOmniVec = this.pages[pageIdx];
                    pageUseOmniVec.release();
                }
            }
        }

        /**
         * ExecutorWithOmniPagesInitial
         */
        public class ExecutorWithOmniPagesInitial {
            private PageUseOmniVec[] pages;
            private int pageCount;
            private int pageSize;

            public ExecutorWithOmniPagesInitial(int pageCount, int pageSize) {
                this.pages = new PageUseOmniVec[pageCount];
                for (int pIdx = 0; pIdx < pageCount; pIdx++) {
                    LongVec[] data = builderGroupBy2CSum2CVecPageLikeQ1(pageSize);
                    this.pages[pIdx] = new PageUseOmniVec(data);
                }
                this.pageCount = pageCount;
                this.pageSize = pageSize;
            }

            /**
             * executor
             */
            public ExecutorWithOmniPagesInitial executor(int loopReadCount) {
                // test read, get vec all data
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseOmniVec pageUseOmniVec = this.pages[pageIdx];
                    for (int vecIdx = 0; vecIdx < 4; vecIdx++) {
                        for (int loop = 0; loop < loopReadCount; loop++) {
                            for (int rIdx = 0; rIdx < pageSize; rIdx++) {
                                pageUseOmniVec.getData()[vecIdx].get(rIdx);
                            }
                        }
                    }
                }
                return this;
            }

            /**
             * release memory
             */
            public void release() {
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseOmniVec pageUseOmniVec = this.pages[pageIdx];
                    pageUseOmniVec.release();
                }
            }
        }

        /**
         * testOmniVecWithInitialAllPage
         */
        public void testOmniVecWithInitialAllPage(int pageCount, int pageSize, int readLoopCount) {
            ExecutorWithOmniPagesInitial executorWithOmniPagesInitial = new ExecutorWithOmniPagesInitial(pageCount,
                    pageSize);
            executorWithOmniPagesInitial.executor(readLoopCount).release();
        }

        /**
         * testHeapVecWithInitialAllPage
         */
        public void testHeapVecWithInitialAllPage(int pageCount, int pageSize, int readLoopCount) {
            ExecutorWithHeapPageInitial executorWithHeapPagesInitial = new ExecutorWithHeapPageInitial(pageCount,
                    pageSize);
            executorWithHeapPagesInitial.executor(readLoopCount).release();
        }

        /**
         * builderGroupBy2CSum2CVecPageLikeQ1
         */
        public LongVec[] builderGroupBy2CSum2CVecPageLikeQ1(int pageSize) {
            LongVec key1 = new LongVec(pageSize);
            LongVec key2 = new LongVec(pageSize);
            LongVec value1 = new LongVec(pageSize);
            LongVec value2 = new LongVec(pageSize);
            for (int i = 0; i < pageSize; i++) {
                key1.set(i, i);
                key2.set(i, i);
                value1.set(i, i);
                value2.set(i, i);
            }

            return new LongVec[]{key1, key2, value1, value2};
        }

        /**
         * builderGroupBy2CSum2CHeapPageLikeQ1
         */
        public HeapLongVec[] builderGroupBy2CSum2CHeapPageLikeQ1(int pageSize) {
            HeapLongVec key1 = new HeapLongVec(pageSize);
            HeapLongVec key2 = new HeapLongVec(pageSize);
            HeapLongVec value1 = new HeapLongVec(pageSize);
            HeapLongVec value2 = new HeapLongVec(pageSize);
            for (int i = 0; i < pageSize; i++) {
                key1.set(i, i);
                key2.set(i, i);
                value1.set(i, i);
                value2.set(i, i);
            }
            return new HeapLongVec[]{key1, key2, value1, value2};
        }

        /**
         * builderGroupBy2CSum2CUnsafevecPageLikeQ1
         */
        public UnsafeLongVec[] builderGroupBy2CSum2CUnsafevecPageLikeQ1(int pageSize) {
            UnsafeLongVec key1 = new UnsafeLongVec(pageSize);
            UnsafeLongVec key2 = new UnsafeLongVec(pageSize);
            UnsafeLongVec value1 = new UnsafeLongVec(pageSize);
            UnsafeLongVec value2 = new UnsafeLongVec(pageSize);
            for (int i = 0; i < pageSize; i++) {
                key1.set(i, i % 2);
                key2.set(i, i % 2);
                value1.set(i, 1);
                value2.set(i, 1);
            }
            return new UnsafeLongVec[]{key1, key2, value1, value2};
        }

        /**
         * release vec memory
         */
        public void release(LongVec[] inputs) {
            for (LongVec vec : inputs) {
                vec.close();
            }
        }

        /**
         * release heap vec memory
         */
        public void release(HeapLongVec[] inputs) {
            for (HeapLongVec vec : inputs) {
                vec.close();
            }
        }

        /**
         * release unsafe long vec memory
         */
        public void release(UnsafeLongVec[] inputs) {
            for (UnsafeLongVec vec : inputs) {
                vec.close();
            }
        }
    }

    /**
     * heap long vec
     */
    public class HeapLongVec {
        private long[] values;

        public HeapLongVec(int pageSize) {
            this.values = new long[pageSize];
        }

        /**
         * set value
         */
        public void set(int idx, long value) {
            this.values[idx] = value;
        }

        /**
         * get value
         */
        public long get(int idx) {
            return this.values[idx];
        }

        /**
         * get size
         */
        public int getSize() {
            return values.length;
        }

        /**
         * close
         */
        public void close() {
        }
    }

    /**
     * unsafe long vec
     */
    public class UnsafeLongVec {
        private final long address;
        private final int capacity;

        public UnsafeLongVec(int capacity) {
            this(JvmUtils.UNSAFE.allocateMemory(Long.BYTES * capacity), capacity);
        }

        public UnsafeLongVec(long address, int capacity) {
            this.address = address;
            this.capacity = capacity;
        }

        /**
         * set value
         */
        public void set(int idx, long value) {
            JvmUtils.UNSAFE.putLong(address + idx * Long.BYTES, value);
        }

        /**
         * get value
         */
        public long get(int idx) {
            return JvmUtils.UNSAFE.getLong(address + idx * Long.BYTES);
        }

        /**
         * get size
         */
        public int getSize() {
            return capacity;
        }

        /**
         * free memory
         */
        public void close() {
            JvmUtils.UNSAFE.freeMemory(address);
        }

        public long getAddress() {
            return address;
        }
    }
}
