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
package nova.hetu.omnicache.vector;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkVecWithPool
{
    private static final int pageSize = 2048;
    private static final int pageCount = 2000;

    @Benchmark
    @Threads(16)
    public void test_omnivec_with_initial_all_page_performance_16()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_omnivec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(32)
    public void test_omnivec_with_initial_all_page_performance_32()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_omnivec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(64)
    public void test_omnivec_with_initial_all_page_performance_64()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_omnivec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(128)
    public void test_omnivec_with_initial_all_page_performance_128()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_omnivec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(16)
    public void test_heapvec_with_initial_all_page_performance_16()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_heapvec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(32)
    public void test_heapvec_with_initial_all_page_performance_32()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_heapvec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(64)
    public void test_heapvec_with_initial_all_page_performance_64()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_heapvec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(128)
    public void test_heapvec_with_initial_all_page_performance_128()
    {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.test_heapvec_with_initial_all_page(pageCount, pageSize, 1);
    }

    @Benchmark
    @Threads(64)
    public void test_omnivec_performance_64()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            LongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_Vec_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }



    @Benchmark
    @Threads(64)
    public void test_heapvec_performance_64()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            HeapLongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_heap_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    @Benchmark
    @Threads(64)
    public void test_unsafevec_performance_64()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            UnsafeLongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_unsafevec_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    @Benchmark
    @Threads(32)
    public void test_omnivec_performance_32()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            LongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_Vec_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    @Benchmark
    @Threads(32)
    public void test_heapvec_performance_32()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            HeapLongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_heap_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    @Benchmark
    @Threads(32)
    public void test_unsafevec_performance_32()
    {
        DataGenerator dataGenerator = new DataGenerator();
        for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
            UnsafeLongVec[] vecs = dataGenerator.builderGroupBy_2C_Sum_2C_unsafevec_Page_like_q1(pageSize);
            for (int i = 0; i < vecs.length; i++) {
                for (int j = 0; j < vecs[i].size(); j++) {
                    vecs[i].get(j);
                }
            }
            dataGenerator.release(vecs);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkVecWithPool.class.getSimpleName() + ".test_.*_with_initial_all_page*")
                .jvmArgs("-Xms2g", "-Xmx16g", "-XX:MaxDirectMemorySize=16g")
                .build();

        new Runner(options).run();
    }

    public class DataGenerator
    {
        private class PageUseOmniVec
        {
            private LongVec[] data;

            public PageUseOmniVec(LongVec[] data)
            {
                this.data = data;
            }

            public LongVec[] getData()
            {
                return this.data;
            }

            public void release()
            {
                for (LongVec vec : data) {
                    vec.close();
                }
            }
        }

        private class PageUseHeapVec
        {
            private HeapLongVec[] data;

            public PageUseHeapVec(HeapLongVec[] data)
            {
                this.data = data;
            }

            public HeapLongVec[] getData()
            {
                return this.data;
            }

            public void release()
            {
                for (HeapLongVec vec : data) {
                    vec.close();
                }
            }
        }

        public class ExecutorWithHeapPageInitial
        {
            private PageUseHeapVec[] pages;
            private int pageCount;
            private int pageSize;

            public ExecutorWithHeapPageInitial(int pageCount, int pageSize)
            {
                this.pages = new PageUseHeapVec[pageCount];
                for (int pIdx = 0; pIdx < pageCount; pIdx++) {
                    HeapLongVec[] data = builderGroupBy_2C_Sum_2C_heap_Page_like_q1(pageSize);
                    this.pages[pIdx] = new PageUseHeapVec(data);
                }
                this.pageCount = pageCount;
                this.pageSize = pageSize;
            }

            public ExecutorWithHeapPageInitial executor(int loopReadCount)
            {
                //test read, get vec all data
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

            public void release()
            {
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseHeapVec pageUseOmniVec = this.pages[pageIdx];
                    pageUseOmniVec.release();
                }
            }
        }

        public class ExecutorWithOmniPagesInitial
        {
            private PageUseOmniVec[] pages;
            private int pageCount;
            private int pageSize;

            public ExecutorWithOmniPagesInitial(int pageCount, int pageSize)
            {
                this.pages = new PageUseOmniVec[pageCount];
                for (int pIdx = 0; pIdx < pageCount; pIdx++) {
                    LongVec[] data = builderGroupBy_2C_Sum_2C_Vec_Page_like_q1(pageSize);
                    this.pages[pIdx] = new PageUseOmniVec(data);
                }
                this.pageCount = pageCount;
                this.pageSize = pageSize;
            }

            public ExecutorWithOmniPagesInitial executor(int loopReadCount)
            {
                //test read, get vec all data
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

            public void release()
            {
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    PageUseOmniVec pageUseOmniVec = this.pages[pageIdx];
                    pageUseOmniVec.release();
                }
            }
        }

        public void test_omnivec_with_initial_all_page(int pageCount, int pageSize, int readLoopCount)
        {
            ExecutorWithOmniPagesInitial executorWithOmniPagesInitial = new ExecutorWithOmniPagesInitial(pageCount, pageSize);
            executorWithOmniPagesInitial.executor(readLoopCount).release();
        }

        public void test_heapvec_with_initial_all_page(int pageCount, int pageSize, int readLoopCount)
        {
            ExecutorWithHeapPageInitial executorWithHeapPagesInitial = new ExecutorWithHeapPageInitial(pageCount, pageSize);
            executorWithHeapPagesInitial.executor(readLoopCount).release();
        }

        public LongVec[] builderGroupBy_2C_Sum_2C_Vec_Page_like_q1(int pageSize)
        {
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

            return new LongVec[] {key1, key2, value1, value2};
        }

        public LongVecWithNoFinalize[] builderGroupBy_2C_Sum_2C_Vec_with_no_finalize_Page_like_q1(int pageSize)
        {
            LongVecWithNoFinalize key1 = new LongVecWithNoFinalize(pageSize);
            LongVecWithNoFinalize key2 = new LongVecWithNoFinalize(pageSize);
            LongVecWithNoFinalize value1 = new LongVecWithNoFinalize(pageSize);
            LongVecWithNoFinalize value2 = new LongVecWithNoFinalize(pageSize);
            for (int i = 0; i < pageSize; i++) {
                key1.set(i, i);
                key2.set(i, i);
                value1.set(i, i);
                value2.set(i, i);
            }

            return new LongVecWithNoFinalize[] {key1, key2, value1, value2};
        }

        public HeapLongVec[] builderGroupBy_2C_Sum_2C_heap_Page_like_q1(int pageSize)
        {
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
            return new HeapLongVec[] {key1, key2, value1, value2};
        }

        public UnsafeLongVec[] builderGroupBy_2C_Sum_2C_unsafevec_Page_like_q1(int pageSize)
        {
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
            return new UnsafeLongVec[] {key1, key2, value1, value2};
        }

        public void release(LongVec[] inputs)
        {
            for (LongVec vec : inputs) {
                vec.close();
            }
        }

        public void release(HeapLongVec[] inputs)
        {
            for (HeapLongVec vec : inputs) {
                vec.close();
            }
        }

        public void release(UnsafeLongVec[] inputs)
        {
            for (UnsafeLongVec vec : inputs) {
                vec.close();
            }
        }

        public void release(LongVecWithNoFinalize[] inputs)
        {
            for (LongVecWithNoFinalize vec : inputs) {
                vec.close();
            }
        }
    }

    public class HeapLongVec
    {
        private long[] values;

        public HeapLongVec(int pageSize)
        {
            this.values = new long[pageSize];
        }

        public void set(int idx, long value)
        {
            this.values[idx] = value;
        }

        public long get(int idx)
        {
            return this.values[idx];
        }

        public int size()
        {
            return values.length;
        }

        public void close()
        {

        }
    }

    public class UnsafeLongVec
    {
        private final long address;
        private final int capacity;

        public UnsafeLongVec(int capacity)
        {
            this(JvmUtils.unsafe.allocateMemory(Long.BYTES * capacity), capacity);
        }

        public UnsafeLongVec(long address, int capacity)
        {

            this.address = address;
            this.capacity = capacity;
        }

        public void set(int idx, long value)
        {
            assert idx < capacity;
            JvmUtils.unsafe.putLong(address + idx * Long.BYTES, value);
        }

        public long get(int idx)
        {
            assert (idx < capacity);
            return JvmUtils.unsafe.getLong(address + idx * Long.BYTES);
        }

        public int size()
        {
            return capacity;
        }

        public void close()
        {
            JvmUtils.unsafe.freeMemory(address);
        }

        public long getAddress()
        {
            return address;
        }
    }

    public class LongVecWithNoFinalize
    {
        private ByteBuffer buffer;
        private int capacity;

        public LongVecWithNoFinalize(int capacity)
        {
            this.capacity = capacity;
            this.buffer = OMVectorBase.allocate(capacity * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        }

        public void set(int idx, long value)
        {
            buffer.putLong(idx * Long.BYTES, value);
        }

        public long get(int idx)
        {
            return buffer.getLong(idx * Long.BYTES);
        }

        public int size()
        {
            return capacity;
        }

        public void close()
        {
            OMVectorBase.release(buffer);
        }
    }
}
