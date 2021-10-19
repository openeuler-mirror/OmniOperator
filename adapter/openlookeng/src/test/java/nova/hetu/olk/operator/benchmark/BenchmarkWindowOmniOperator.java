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
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DummySpillerFactory;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.window.AggregateWindowFunction;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.operator.WindowOmniOperator;
import nova.hetu.olk.tool.VecAllocatorHelper;
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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.TestWindowOperator.ROW_NUMBER;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.prestosql.sql.tree.WindowFrame.Type.RANGE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static nova.hetu.olk.operator.benchmark.BenchmarkWindowOmniOperator.Context.ROWS_PER_PAGE;
import static nova.hetu.olk.operator.benchmark.BenchmarkWindowOmniOperator.Context.TOTAL_PAGES;
import static nova.hetu.olk.operator.TestWindowOmniOperator.RANK;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkWindowOmniOperator {
    @State(Thread)
    public static class Context {
        public static final int NUMBER_OF_GROUP_COLUMNS = 2;
        public static final int TOTAL_PAGES = 100;
        public static final int ROWS_PER_PAGE = 10000;

        private static final Metadata metadata = createTestMetadataManager();
        private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty());
        public static final List<WindowFunctionDefinition> COUNT_BIGINT_GROUP2 = ImmutableList.of(
                window(AggregateWindowFunction.supplier(new Signature("count",
                        FunctionKind.AGGREGATE,
                        BigintType.BIGINT.getTypeSignature(), BIGINT.getTypeSignature()), metadata.getAggregateFunctionImplementation(
                        new Signature("count", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))), BIGINT, UNBOUNDED_FRAME, 2));
        public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP3 = ImmutableList.of(
                window(AggregateWindowFunction.supplier(new Signature("avg",
                        FunctionKind.AGGREGATE,
                        DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()), metadata.getAggregateFunctionImplementation(
                        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))), BIGINT, UNBOUNDED_FRAME, 7));
        public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP5 = ImmutableList.of(
                window(AggregateWindowFunction.supplier(new Signature("avg",
                        FunctionKind.AGGREGATE,
                        DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()), metadata.getAggregateFunctionImplementation(
                        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))), BIGINT, UNBOUNDED_FRAME, 1));
        public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP6 = ImmutableList.of(
                window(AggregateWindowFunction.supplier(new Signature("avg",
                        FunctionKind.AGGREGATE,
                        DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()), metadata.getAggregateFunctionImplementation(
                        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))), BIGINT, UNBOUNDED_FRAME, 1));
        public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP7 = ImmutableList.of(
                window(AggregateWindowFunction.supplier(new Signature("avg",
                        FunctionKind.AGGREGATE,
                        DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()), metadata.getAggregateFunctionImplementation(
                        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))), BIGINT, UNBOUNDED_FRAME, 6));

        private static final Map<String, ImmutableList<Integer>> PARTITION_CHANNELS =
                ImmutableMap.<String, ImmutableList<Integer>>builder()
                        .put("group1", ImmutableList.of(0, 1))
                        .put("group2", ImmutableList.of(0, 1, 2))
                        .put("group3", ImmutableList.of(0, 1, 2, 3, 4))
                        .put("group4", ImmutableList.of(0, 1, 2, 3))
                        .put("group5", ImmutableList.of(1))
                        .put("group6", ImmutableList.of(1))
                        .put("group7", ImmutableList.of(0, 2, 3, 4))
                        .build();

        private static final Map<String, ImmutableList<Type>> INPUT_TYPES =
                ImmutableMap.<String, ImmutableList<Type>>builder()
                        .put("group1", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))
                        .put("group2", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))
                        .put("group3", ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50), createVarcharType(50), INTEGER, INTEGER, BIGINT, BIGINT))
                        .put("group4", ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50), createVarcharType(50), INTEGER, INTEGER, BIGINT))
                        .put("group5", ImmutableList.of(BIGINT, INTEGER))
                        .put("group6", ImmutableList.of(BIGINT, INTEGER))
                        .put("group7", ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50), createVarcharType(50), createVarcharType(50), INTEGER, BIGINT))
                        .build();
        private static final Map<String, List<WindowFunctionDefinition>> WINDOW_TYPES =
                ImmutableMap.<String, List<WindowFunctionDefinition>>builder()
                        .put("group1", ROW_NUMBER)
                        .put("group2", COUNT_BIGINT_GROUP2)
                        .put("group3", AVG_BIGINT_GROUP3)
                        .put("group4", RANK)
                        .put("group5", AVG_BIGINT_GROUP5)
                        .put("group6", AVG_BIGINT_GROUP6)
                        .put("group7", AVG_BIGINT_GROUP7)
                        .build();
        private static final Map<String, List<Integer>> SORT_CHANNELS =
                ImmutableMap.<String, List<Integer>>builder()
                        .put("group1", ImmutableList.of(3))
                        .put("group2", ImmutableList.of(3))
                        .put("group3", ImmutableList.of())
                        .put("group4", ImmutableList.of(4, 5))
                        .put("group5", ImmutableList.of())
                        .put("group6", ImmutableList.of())
                        .put("group7", ImmutableList.of())
                        .build();

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7"})
        String testGroup;

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        public int rowsPerPartition;

        @Param({"0"})
        public int numberOfPregroupedColumns;

        public int partitionsPerGroup;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        private List<Page> pages = new ArrayList<>();

        @Setup
        public void setup() {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            createOperatorFactoryAndGenerateTestData(numberOfPregroupedColumns);
        }

        @TearDown
        public void cleanup() {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private void createOperatorFactoryAndGenerateTestData(int numberOfPreGroupedColumns) {
            pages = generateTestData();

            List<Integer> partitionChannels = PARTITION_CHANNELS.get(testGroup);
            List<WindowFunctionDefinition> windowType = WINDOW_TYPES.get(testGroup);
            List<Type> inputTypes = INPUT_TYPES.get(testGroup);
            List<Integer> sortChannels = SORT_CHANNELS.get(testGroup);
            List<Integer> outputChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();

            for (int i = 0; i < inputTypes.size(); i++) {
                outputChannels.add(i);
            }

            for (int i = 0; i < sortChannels.size(); i++) {
                sortOrders.add(SortOrder.ASC_NULLS_LAST);
            }

            if (numberOfPreGroupedColumns == 0) {
                // Ungrouped
                operatorFactory = createFactoryUnbounded(
                        inputTypes,
                        outputChannels,
                        windowType,
                        partitionChannels,
                        Ints.asList(),
                        sortChannels,
                        sortOrders,
                        0,
                        new DummySpillerFactory(),
                        false);
            } else if (numberOfPreGroupedColumns < NUMBER_OF_GROUP_COLUMNS) {
                // Partially grouped
                operatorFactory = createFactoryUnbounded(
                        inputTypes,
                        outputChannels,
                        windowType,
                        partitionChannels,
                        Ints.asList(1),
                        sortChannels,
                        sortOrders,
                        0,
                        new DummySpillerFactory(),
                        false);
            } else {
                // Fully grouped and (potentially) sorted
                operatorFactory = createFactoryUnbounded(
                        inputTypes,
                        outputChannels,
                        windowType,
                        partitionChannels,
                        Ints.asList(0, 1),
                        sortChannels,
                        sortOrders,
                        (numberOfPreGroupedColumns - NUMBER_OF_GROUP_COLUMNS),
                        new DummySpillerFactory(),
                        false);
            }
        }

        private WindowOmniOperator.WindowOmniOperatorFactory createFactoryUnbounded(
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> functions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                boolean spillEnabled) {
            return new WindowOmniOperator.WindowOmniOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    sourceTypes,
                    outputChannels,
                    functions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    10);
        }

        public static WindowOmniOperator.WindowOmniOperatorFactory createFactoryUnbounded(
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> functions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                SpillerFactory spillerFactory,
                boolean spillEnabled) {
            return new WindowOmniOperator.WindowOmniOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    sourceTypes,
                    outputChannels,
                    functions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    10);
        }

        private List<Page> generateTestData() {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            List<Page> pages = buildPages(typesArray, dictionaryBlocks);

            return transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
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

        public TaskContext createTaskContext() {
            TaskContext testingTaskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
            VecAllocatorHelper.setVectorAllocatorToTaskContext(testingTaskContext, VecAllocator.GLOBAL_VECTOR_ALLOCATOR);
            return testingTaskContext;
        }

        public OperatorFactory getOperatorFactory() {
            return operatorFactory;
        }

        public List<Page> getPages() {
            List<Page> slicedPages = new ArrayList<>();
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }
    }

    @Benchmark
    public List<Page> benchmark(BenchmarkWindowOmniOperator.Context context) {
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
                } else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        List<Page> pages = outputPages.build();

        for (Page page : pages) {
            page.close();
        }

        return pages;
    }

    @Test
    public void verifyUnGroupedWithMultiplePartitions() {
        verify(10, 0, false);
    }

    @Test
    public void verifyUnGroupedWithSinglePartition() {
        verify(10, 0, true);
    }

    @Test
    public void verifyPartiallyGroupedWithMultiplePartitions() {
        verify(10, 1, false);
    }

    @Test
    public void verifyPartiallyGroupedWithSinglePartition() {
        verify(10, 1, true);
    }

    @Test
    public void verifyFullyGroupedWithMultiplePartitions() {
        verify(10, 2, false);
    }

    @Test
    public void verifyFullyGroupedWithSinglePartition() {
        verify(10, 2, true);
    }

    @Test
    public void verifyFullyGroupedAndFullySortedWithMultiplePartitions() {
        verify(10, 3, false);
    }

    @Test
    public void verifyFullyGroupedAndFullySortedWithSinglePartition() {
        verify(10, 3, true);
    }

    private void verify(
            int numberOfRowsPerPartition,
            int numberOfPreGroupedColumns,
            boolean useSinglePartition) {
        BenchmarkWindowOmniOperator.Context context = new BenchmarkWindowOmniOperator.Context();

        context.rowsPerPartition = numberOfRowsPerPartition;
        context.numberOfPregroupedColumns = numberOfPreGroupedColumns;

        if (useSinglePartition) {
            context.partitionsPerGroup = 1;
            context.rowsPerPartition = ROWS_PER_PAGE;
        }

        context.setup();

        Assert.assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            Assert.assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        benchmark(context);

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkWindowOmniOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
