/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.operator.DistinctLimitOmniOperator;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
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
public class BenchmarkDistinctLimitOmniOperator {
    @State(Scope.Thread)
    public static class BenchmarkContext {
        @Param({"1", "100", "10000", "100000"})
        private String limit = "100";

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7", "group8"})
        String testGroup = "group1";

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Param({"32", "1024"})
        public String ROWS_PER_PAGE_STR = "1024";

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        public static final int TOTAL_PAGES = 1000;

        private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
                .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(INTEGER))
                .put("group2", ImmutableList.of(createVarcharType(16))).put("group3", ImmutableList.of(DOUBLE))
                .put("group4", ImmutableList.of(createDecimalType()))
                .put("group5", ImmutableList.of(INTEGER, createVarcharType(16)))
                .put("group6", ImmutableList.of(INTEGER, BIGINT, createDecimalType(), DOUBLE))
                .put("group7", ImmutableList.of(createVarcharType(20), createVarcharType(30), createVarcharType(50)))
                .put("group8", ImmutableList.of(INTEGER, createVarcharType(30), BIGINT, createDecimalType(),
                        createVarcharType(50)))
                .build();

        private static final Map<String, List<Integer>> DISTINCT_CHANNELS = ImmutableMap
                .<String, List<Integer>>builder().put("group1", ImmutableList.of(0)).put("group2", ImmutableList.of(0))
                .put("group3", ImmutableList.of(0)).put("group4", ImmutableList.of(0))
                .put("group5", ImmutableList.of(0, 1)).put("group6", ImmutableList.of(0, 1, 2, 3))
                .put("group7", ImmutableList.of(0, 1, 2)).put("group8", ImmutableList.of(0, 1, 2, 3, 4)).build();

        @Setup
        public void setup() {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            createOperatorFactoryAndGenerateTestData();
        }

        private void createOperatorFactoryAndGenerateTestData() {
            pages = generateTestData();

            List<Type> inputTypes = INPUT_TYPES.get(testGroup);
            List<Integer> distinctChannels = DISTINCT_CHANNELS.get(testGroup);

            operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(0, new PlanNodeId("test"),
                    inputTypes, distinctChannels, Optional.empty(), Integer.valueOf(limit));
        }

        @TearDown
        public void cleanup() {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        public TaskContext createTaskContext() {
            TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION,
                    new DataSize(2, GIGABYTE));
            VecAllocatorHelper.setVectorAllocatorToTaskContext(taskContext, VecAllocator.GLOBAL_VECTOR_ALLOCATOR);
            return taskContext;
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
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(typesArray,
                            Integer.valueOf(ROWS_PER_PAGE_STR)));
                } else {
                    pages.add(PageBuilderUtil.createSequencePage(typesArray, Integer.valueOf(ROWS_PER_PAGE_STR)));
                }
            }
            return pages;
        }
    }

    @Benchmark
    public List<Page> distinctLimit(BenchmarkContext context) {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false)
                .addDriverContext();
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
                outputPage.close();
            }
        }

        return outputPages.build();
    }

    @Test
    public void verify() {
        BenchmarkContext context = new BenchmarkContext();
        context.limit = "123";
        context.setup();

        List<Page> outputPages = distinctLimit(context);
        assertEquals(123, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    @Test
    public void verifyDitionary() {
        BenchmarkContext context = new BenchmarkContext();
        context.limit = "123";
        context.dictionaryBlocks = true;
        context.testGroup = "group5";
        context.setup();

        List<Page> outputPages = distinctLimit(context);
        assertEquals(123, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDistinctLimitOmniOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
