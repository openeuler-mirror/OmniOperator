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
package nova.hetu.olk.operator.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.operator.*;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkTopNOlkOperator
{

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param({ "1","10","100", "1000", "10000"})
        private String topN = "100";

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7", "group8", "group9", "group10"})
        String testGroup = "group1";

        @Param({"false","true"})
        boolean dictionaryBlocks;
        @Param({"32","1024"})
        public String ROWS_PER_PAGE_STR = "1024";

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        public static final int TOTAL_PAGES =1000;

        private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap.<String,ImmutableList<Type>>builder()
                .put("group1", ImmutableList.of(createVarcharType(16)))
                .put("group2", ImmutableList.of(INTEGER,INTEGER))
                .put("group3", ImmutableList.of(INTEGER,INTEGER,INTEGER))
                .put("group4", ImmutableList.of(BIGINT,INTEGER))
                .put("group5", ImmutableList.of(createVarcharType(16)))
                .put("group6", ImmutableList.of(INTEGER,INTEGER,INTEGER))
                .put("group7", ImmutableList.of(createVarcharType(20),createVarcharType(30),createVarcharType(50)))
                .put("group8", ImmutableList.of(createVarcharType(50),INTEGER))
                .put("group9", ImmutableList.of(INTEGER,createVarcharType(60),createVarcharType(20),createVarcharType(30)))
                .put("group10", ImmutableList.of(INTEGER,createVarcharType(50),INTEGER,INTEGER,createVarcharType(50)))
                .build();

        private static final Map<String, List<Integer>> SORT_CHANNELS = ImmutableMap.<String, List<Integer>>builder()
                .put("group1", ImmutableList.of(0))
                .put("group2", ImmutableList.of(0,1))
                .put("group3", ImmutableList.of(0,1,2))
                .put("group4", ImmutableList.of(0,1))
                .put("group5", ImmutableList.of(0))
                .put("group6", ImmutableList.of(0,1,2))
                .put("group7", ImmutableList.of(0,1,2))
                .put("group8", ImmutableList.of(0,1))
                .put("group9", ImmutableList.of(0,1,2,3))
                .put("group10", ImmutableList.of(0,1,2,3))
                .build();



//            private static final Map<String, List<SortOrder>> SORT_ORDERS= ImmutableMap.of(
//                "group1", ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST,DESC_NULLS_LAST, ASC_NULLS_FIRST),
//                "group2", ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST,DESC_NULLS_LAST, ASC_NULLS_FIRST));
        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            createOperatorFactoryAndGenerateTestData();
        }

        private void createOperatorFactoryAndGenerateTestData( ) {
            pages = generateTestData();

            List<Type> inputTypes = INPUT_TYPES.get(testGroup);
            List<Integer> sortChannels = SORT_CHANNELS.get(testGroup);
            List<SortOrder> sortOrders = new ArrayList<>();
            for (int i = 0; i < sortChannels.size(); i++) {
                sortOrders.add(SortOrder.ASC_NULLS_LAST);
            }
                // Ungrouped
                operatorFactory = createFactoryUnbounded(
                        inputTypes,
                        sortChannels,
                        sortOrders);
        }

        private TopNOperator.TopNOperatorFactory createFactoryUnbounded(
                List<? extends Type> sourceTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder
        ) {
            return new TopNOperator.TopNOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    sourceTypes,
                    Integer.valueOf(topN),
                    sortChannels,
                    sortOrder);
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        public TaskContext createTaskContext()
        {
            TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION,
                    new DataSize(2, GIGABYTE));
            VecAllocatorHelper.setVectorAllocatorToTaskContext(taskContext, VecAllocator.GLOBAL_VECTOR_ALLOCATOR);
            return taskContext;
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        private List<Page> generateTestData() {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            int currentPartitionIdentifier = 1;
            List<Page> pages = buildPages(currentPartitionIdentifier, typesArray, dictionaryBlocks);

            return transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
        }

        private List<Page> buildPages(int currentPartitionIdentifier, List<Type> typesArray, boolean dictionaryBlocks) {
            List<Page> pages = new ArrayList<>();
            for (int i = 0; i < TOTAL_PAGES; i++) {
                if (dictionaryBlocks) {
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(typesArray, Integer.valueOf(ROWS_PER_PAGE_STR)));
                } else {
                    pages.add(PageBuilderUtil.createSequencePage(typesArray, Integer.valueOf(ROWS_PER_PAGE_STR)));
                }
            }
            return pages;
        }
    }

    @Benchmark
    public List<Page> topN(BenchmarkContext context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void verify()
    {
        BenchmarkContext context = new BenchmarkContext();
        context.topN = "123";
        context.setup();

        List<Page> outputPages = topN(context);
        assertEquals(123, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args)
        throws RunnerException
    {
        BenchmarkContext data = new BenchmarkContext();
        data.setup();
        new BenchmarkTopNOlkOperator().topN(data);

        Options options = new OptionsBuilder()
            .verbosity(VerboseMode.NORMAL)
            .include(".*" + BenchmarkTopNOlkOperator.class.getSimpleName() + ".*")
            .result("resultolk.csv").resultFormat(ResultFormatType.CSV)
            .build();

        new Runner(options).run();
    }
}
