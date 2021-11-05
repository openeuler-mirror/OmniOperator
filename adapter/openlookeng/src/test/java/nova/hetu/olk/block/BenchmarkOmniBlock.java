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
package nova.hetu.olk.block;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.benchmark.PageBuilderUtil;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
public class BenchmarkOmniBlock {
    @State(Thread)
    public static class Context {
        public static final int NUMBER_OF_GROUP_COLUMNS = 2;
        public static final int TOTAL_PAGES = 100;
        public static final int ROWS_PER_PAGE = 10000;

        private static final Map<String, ImmutableList<Type>> INPUT_TYPES =
                ImmutableMap.<String, ImmutableList<Type>>builder()
                        .put("long", ImmutableList.of(BIGINT))
                        .put("int", ImmutableList.of(INTEGER))
                        .put("double", ImmutableList.of(DOUBLE))
                        .put("varchar",  ImmutableList.of(createVarcharType(50)))
                        .build();

        @Param({"int", "long", "double", "varchar"})
        String dataType;

        @Param({"false"})
        boolean dictionaryBlocks;

        private List<Page> pages = new ArrayList<>();
        private List<Page> offHeapPages = new ArrayList<>();

        private static int[] positions;

        private static Random random = new Random();

        static {
            generatePositionIds();
        }

        @Setup
        public void setup() {
            pages = generateTestData();
            offHeapPages = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
        }

        @TearDown
        public void cleanup() {
        }

        private List<Page> generateTestData() {
            List<Type> typesArray = INPUT_TYPES.get(dataType);
            return buildPages(typesArray, dictionaryBlocks);
        }

        private static void generatePositionIds() {
            Random random = new Random();
            positions = new int[ROWS_PER_PAGE / 2];
            for (int i = 0; i < positions.length; i++) {
                positions[i] = Math.abs(random.nextInt() % ROWS_PER_PAGE);
            }
        }

        private List<Page> buildPages(List<Type> typesArray, boolean dictionaryBlocks) {
            List<Page> pages = new ArrayList<>();
            for (int i = 0; i < TOTAL_PAGES; i++) {
                if (dictionaryBlocks) {
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(typesArray, ROWS_PER_PAGE));
                } else {
                    pages.add(PageBuilderUtil.createSequencePage(typesArray, ROWS_PER_PAGE));
                }
            }
            return pages;
        }

        public List<Page> getPages() {
            return pages;
        }

        public List<Page> getOffHeapPage() {
            List<Page> slicedPages = new ArrayList<>();
            for (Page page : offHeapPages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }

        public int[] getPositionIds() {
            return positions;
        }

    }

    @Benchmark
    public List<Block> omniBlockCopyPositions(BenchmarkOmniBlock.Context context) {
        List<Page> offHeapPages = context.getOffHeapPage();
        List<Block> blocks = new ArrayList<>();
        for (Page page : offHeapPages) {
            int positionCount = page.getPositionCount();
            int channelCount = page.getChannelCount();
            List<Block> newBlocks = new ArrayList<>();
            for (int i = 0; i < channelCount; i++) {
                Block originalBlock = page.getBlock(i);
                Block block = originalBlock.copyPositions(context.getPositionIds(), 0, positionCount / 2);
                newBlocks.add(block);
            }
            page.close();
            for (Block block : newBlocks) {
                block.close();
            }
            blocks.addAll(newBlocks);
        }
        return blocks;
    }

    @Benchmark
    public List<Block> blockCopyPositions(BenchmarkOmniBlock.Context context) {
        List<Page> heapPages = context.getPages();
        List<Block> blocks = new ArrayList<>();
        for (Page page : heapPages) {
            int positionCount = page.getPositionCount();
            int channelCount = page.getChannelCount();
            for (int i = 0; i < channelCount; i++) {
                Block block = page.getBlock(i).copyPositions(context.getPositionIds(), 0, positionCount / 2);
                blocks.add(block);
            }
        }
        return blocks;
    }

    private void closePages(List<Page> pages) {
        for (Page page : pages) {
            page.close();
        }
    }

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkOmniBlock.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
