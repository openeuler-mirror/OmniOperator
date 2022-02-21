/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.RowPagesBuilder;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.StreamingAggregationOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.operator.HashAggregationOmniOperator;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;
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
import org.testng.annotations.Test;

import javax.activation.UnsupportedDataTypeException;

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
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(0)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashAggregationOmniOperator
{
    private static final Metadata metadata = createTestMetadataManager();
    private static final VarcharType FIXED_WIDTH_VARCHAR = VarcharType.createVarcharType(200);

    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
            new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    private static final Map<String, List<Type>> allTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, INTEGER, INTEGER, BIGINT))
            .put("sql4", ImmutableList.of(FIXED_WIDTH_VARCHAR, INTEGER, INTEGER, INTEGER, BIGINT))
            .put("sql6", ImmutableList.of(INTEGER, INTEGER, BIGINT))
            .put("sql7", ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT, BIGINT, FIXED_WIDTH_VARCHAR, BIGINT, BIGINT))
            .build();
    private static final Map<String, List<Integer>> channels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(0, 1, 2, 3, 4, 5, 6))
            .put("sql4", ImmutableList.of(0, 1, 2, 3, 4))
            .put("sql6", ImmutableList.of(0, 1, 2))
            .put("sql7", ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7))
            .put("sql9", ImmutableList.of(0, 1, 2, 3, 4, 5))
            .build();
    private static final Map<String, List<Integer>> hashChannels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(0, 1, 2, 3, 4, 5))
            .put("sql4", ImmutableList.of(0, 1, 2, 3))
            .put("sql6", ImmutableList.of(0, 1))
            .put("sql7", ImmutableList.of(0, 1, 2))
            .put("sql9", ImmutableList.of(0, 1, 2, 3))
            .build();
    private static final Map<String, List<Type>> hashTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, INTEGER, INTEGER))
            .put("sql4", ImmutableList.of(FIXED_WIDTH_VARCHAR, INTEGER, INTEGER, INTEGER))
            .put("sql6", ImmutableList.of(INTEGER, INTEGER))
            .put("sql7", ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT, BIGINT, FIXED_WIDTH_VARCHAR))
            .build();
    private static final Map<String, List<Integer>> aggChannels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(6))
            .put("sql4", ImmutableList.of(4))
            .put("sql6", ImmutableList.of(2))
            .put("sql7", ImmutableList.of(3, 4, 5, 6, 7))
            .put("sql9", ImmutableList.of(4, 5))
            .build();
    private static final Map<String, List<Type>> aggInputTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(BIGINT))
            .put("sql4", ImmutableList.of(BIGINT))
            .put("sql6", ImmutableList.of(BIGINT))
            .put("sql7", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT))
            .build();
    private static final Map<String, List<Type>> aggOutputTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(BIGINT))
            .put("sql4", ImmutableList.of(BIGINT))
            .put("sql6", ImmutableList.of(BIGINT))
            .put("sql7", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT))
            .build();
    private static final Map<String, List<String>> aggFuncTypes = new ImmutableMap.Builder<String, List<String>>()
            .put("sql2", ImmutableList.of("sum"))
            .put("sql4", ImmutableList.of("sum"))
            .put("sql6", ImmutableList.of("sum"))
            .put("sql7", ImmutableList.of("sum", "sum", "sum", "sum", "sum"))
            .put("sql9", ImmutableList.of("sum", "sum"))
            .build();

    public static final int TOTAL_PAGES = 140;
    public static final int ROWS_PER_PAGE = 10_000;
    public static final String PREFIX_BASE = "A";

    @Param({"sql2", "sql4", "sql6", "sql7", "sql9"})
    public static String sqlId;

    @State(Thread)
    public static class Context
    {
        @Param({"100", "1000", "10000"})
        public int rowsPerGroup;

        @Param({"hash"})
        public String operatorType;

        @Param({"false", "true"})
        public boolean isDictionary;

        @Param({"0", "50", "150"})
        public int prefixLength;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;
        private List<Operator> operators;
        private List<Page> outputPages;
        private VecAllocator allocator;

        private String genVarcharPrefix()
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < prefixLength; ++i) {
                stringBuilder.append(PREFIX_BASE);
            }
            return stringBuilder.toString();
        }

        private List<Page> buildPages()
                throws UnsupportedDataTypeException
        {
            List<Type> types = allTypes.get(sqlId);

            int groupsPerPage = ROWS_PER_PAGE / rowsPerGroup;
            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(false, ImmutableList.of(0), types);
            String prefix = genVarcharPrefix();

            for (int i = 0; i < TOTAL_PAGES; i++) {
                List<Block> allBlocks = new ArrayList<>();
                for (int j = 0; j < types.size(); j++) {
                    BlockBuilder blockBuilder;
                    switch (types.get(j).getDisplayName()) {
                        case "varchar":
                            if (!isDictionary) {
                                blockBuilder = VARCHAR.createBlockBuilder(null, ROWS_PER_PAGE, 200);
                                for (int k = 0; k < groupsPerPage; k++) {
                                    String groupKey = format(prefix + "%s", i * groupsPerPage + k);
                                    repeatToStringBlock(groupKey, rowsPerGroup, blockBuilder);
                                }
                                allBlocks.add(blockBuilder.build());
                            }
                            else {
                                allBlocks.add(createVarcharDictionary(i, prefix, groupsPerPage));
                            }
                            break;
                        case "bigint":
                            if (!isDictionary) {
                                blockBuilder = BIGINT.createBlockBuilder(null, ROWS_PER_PAGE);
                                for (int k = 0; k < groupsPerPage; k++) {
                                    long groupKey = i * groupsPerPage + k;
                                    repeatToLongBlock(groupKey, rowsPerGroup, blockBuilder);
                                }
                                allBlocks.add(blockBuilder.build());
                            }
                            else {
                                allBlocks.add(createLongDictionary(i, groupsPerPage));
                            }
                            break;
                        case "integer":
                            if (!isDictionary) {
                                blockBuilder = INTEGER.createBlockBuilder(null, ROWS_PER_PAGE);
                                for (int k = 0; k < groupsPerPage; k++) {
                                    long groupKey = i * groupsPerPage + k;
                                    repeatToIntegerBlock(groupKey, rowsPerGroup, blockBuilder);
                                }
                                allBlocks.add(blockBuilder.build());
                            }
                            else {
                                allBlocks.add(createIntegerDictionary(i, groupsPerPage));
                            }
                            break;
                        default:
                            throw new UnsupportedDataTypeException(" unknown data type" + types.get(j).getDisplayName());
                    }
                }
                pagesBuilder.addBlocksPage(allBlocks.toArray(new Block[allBlocks.size()]));
            }
            return pagesBuilder.build();
        }

        private FunctionType[] transferAggType(List<String> aggregators)
        {
            FunctionType[] res = new FunctionType[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                // aggregator type, eg:sum,avg...
                String agg = aggregators.get(i);
                switch (agg) {
                    case "sum":
                        res[i] = OMNI_AGGREGATION_TYPE_SUM;
                        break;
                    case "avg":
                        res[i] = OMNI_AGGREGATION_TYPE_AVG;
                        break;
                    case "count":
                        res[i] = OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
                        break;
                    case "min":
                        res[i] = OMNI_AGGREGATION_TYPE_MIN;
                        break;
                    case "max":
                        res[i] = OMNI_AGGREGATION_TYPE_MAX;
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "unsupported Aggregator type by OmniRuntime: " + agg);
                }
            }
            return res;
        }

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed("test-scheduledExecutor-%s"));
            operators = new ArrayList<>();
            outputPages = new ArrayList<>();
            allocator = VecAllocatorFactory.create("benchmark", null);

            boolean hashAggregation = operatorType.equalsIgnoreCase("hash");

            try {
                pages = buildPages();
            }
            catch (UnsupportedDataTypeException e) {
                e.printStackTrace();
            }
            // need to transfer to off-heap
            pages = OperatorUtils.transferToOffHeapPages(allocator, this.pages);

            if (hashAggregation) {
                operatorFactory = createHashAggregationOperatorFactory();
            }
            else {
                operatorFactory = createStreamingAggregationOperatorFactory();
            }
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
            for (Operator op : operators) {
                try {
                    op.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (Page page : outputPages) {
                page.close();
            }
            operators.clear();
            outputPages.clear();
            allocator.close();
        }

        private OperatorFactory createStreamingAggregationOperatorFactory()
        {
            return new StreamingAggregationOperator.StreamingAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(0),
                    AggregationNode.Step.SINGLE,
                    ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                            LONG_SUM.bind(ImmutableList.of(1), Optional.empty())),
                    new JoinCompiler(createTestMetadataManager()));
        }

        private OperatorFactory createHashAggregationOperatorFactory()
        {
            return new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(0, new PlanNodeId("test"), allTypes.get(sqlId),
                    Ints.toArray(hashChannels.get(sqlId)), OperatorUtils.toVecTypes(hashTypes.get(sqlId)),
                    Ints.toArray(aggChannels.get(sqlId)), OperatorUtils.toVecTypes(aggInputTypes.get(sqlId)),
                    transferAggType(aggFuncTypes.get(sqlId)), OperatorUtils.toVecTypes(aggOutputTypes.get(sqlId)),
                    AggregationNode.Step.SINGLE);
        }

        private static void repeatToStringBlock(String value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                VARCHAR.writeString(blockBuilder, value);
            }
        }

        private static void repeatToLongBlock(long value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }

        private static void repeatToIntegerBlock(long value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                INTEGER.writeLong(blockBuilder, value);
            }
        }

        private static Block createVarcharDictionary(int pageId, String prefix, int groupCount)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                String groupKey = format(prefix + "%s", pageId * groupCount + k);
                VARCHAR.writeString(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
        }

        private static Block createLongDictionary(int pageId, int groupCount)
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                long groupKey = pageId * groupCount + k;
                BIGINT.writeLong(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
        }

        private static Block createIntegerDictionary(int pageId, int groupCount)
        {
            BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                int groupKey = pageId * groupCount + k;
                INTEGER.writeLong(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
        }

        public TaskContext createTaskContext()
        {
            TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
            VecAllocatorHelper.setVectorAllocatorToTaskContext(taskContext, allocator);
            return taskContext;
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            List<Page> slicedPages = new ArrayList<>();
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }
    }

    @Benchmark
    public List<Page> benchmark(BenchmarkHashAggregationOmniOperator.Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);
        context.operators.add(operator);

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

        List<Page> pages = outputPages.build();
        for (Page page : pages) {
            page.close();
        }
        return pages;
    }

    @Test
    public void verifyStreaming()
    {
        verify(1, "streaming");
        verify(10, "streaming");
        verify(1000, "streaming");
    }

    @Test
    public void verifyHash()
    {
        verify(1, "hash");
        verify(10, "hash");
        verify(1000, "hash");
    }

    private void verify(int rowsPerGroup, String operatorType)
    {
        BenchmarkHashAggregationOmniOperator.Context context = new BenchmarkHashAggregationOmniOperator.Context();
        context.operatorType = operatorType;
        context.rowsPerGroup = rowsPerGroup;
        context.setup();

        assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        List<Page> outputPages = benchmark(context);
        assertEquals(TOTAL_PAGES * ROWS_PER_PAGE / rowsPerGroup, outputPages.stream().mapToInt(Page::getPositionCount).sum());
        for (Page page : outputPages) {
            page.close();
        }

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashAggregationOmniOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
