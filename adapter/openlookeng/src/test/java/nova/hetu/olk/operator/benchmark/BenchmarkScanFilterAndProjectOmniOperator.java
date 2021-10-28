/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */
package nova.hetu.olk.operator.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.SequencePageBuilder;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.operator.*;
import io.prestosql.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.FunctionAssertions.createExpression;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.prestosql.testing.TestingSplit.createLocalSplit;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 20)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkScanFilterAndProjectOmniOperator {

    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);

    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final TypeAnalyzer TYPE_ANALYZER = new TypeAnalyzer(new SqlParser(), METADATA);

    private static final int TOTAL_POSITIONS = 1_000_000;
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = new DataSize(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    @State(Thread)
    public static class Context
    {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;


        @Param({"1024"})
        int positionsPerPage = 1024;

//        @Param({"2", "4", "8", "16", "32"})
        @Param({"2", "4", "8"})
        int columnCount = 2;

        @Param({"varchar"})
        String type = "varchar";

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            Type type = TYPE_MAP.get(this.type);

            for (int i = 0; i < columnCount; i++) {
                Symbol symbol = new Symbol(type.getDisplayName().toLowerCase(ENGLISH) + i);
                symbolTypes.put(symbol, type);
                sourceLayout.put(symbol, i);
            }

            List<RowExpression> projections = getProjections(type);
            List<Type> types = projections.stream().map(RowExpression::getType).collect(toList());
            List<ColumnHandle> columnHandles = IntStream.range(0, columnCount)
                    .mapToObj(i -> new TestingColumnHandle(Integer.toString(i)))
                    .collect(toImmutableList());

            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(METADATA, 0);
//            PageProcessor pageProcessor = new ExpressionCompiler(METADATA, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections).get();

            PageProcessor pageProcessor = new OmniExpressionCompiler(METADATA, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections, Optional.empty(),
                    OptionalInt.empty(), types, new TaskId("test")).get();

            CursorProcessor cursorProcessor = new ExpressionCompiler(METADATA, pageFunctionCompiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();

            createTaskContext();
            createScanFilterAndProjectOperatorFactories(createInputPages(types), pageProcessor, cursorProcessor, columnHandles, types);
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private void createScanFilterAndProjectOperatorFactories(List<Page> inputPages, PageProcessor pageProcessor, CursorProcessor cursorProcessor, List<ColumnHandle> columnHandles, List<Type> types)
        {
            operatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("test_source"),
                    (session, split, table, columns, dynamicFilter) -> new FixedPageSource(inputPages),
                    () -> cursorProcessor,
                    () -> pageProcessor,
                    TEST_TABLE_HANDLE,
                    columnHandles,
                    null,
                    types,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT,
                    ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                    0,
                    false,
                    Optional.empty(),
                    0,
                    0);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        private List<Page> createInputPages(List<Type> types)
        {
            ImmutableList.Builder<Page> inputPagesBuilder = ImmutableList.builder();
            for (int i = 0; i < TOTAL_POSITIONS / positionsPerPage; ++i) {
                inputPagesBuilder.add(createPage(types, positionsPerPage, dictionaryBlocks));
            }
            List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, inputPagesBuilder.build());
            return offHeapPages;
        }

        private RowExpression getFilter(Type type)
        {
            if (type == VARCHAR) {
//                return rowExpression("cast(varchar0 as bigint) % 2 = 0");
                return rowExpression("varchar0 = '100'");
            }
            if (type == BIGINT) {
                return rowExpression("bigint0 % 2 = 0");
            }
            throw new IllegalArgumentException("filter not supported for type : " + type);
        }

        private List<RowExpression> getProjections(Type type)
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            if (type == BIGINT) {
                for (int i = 0; i < columnCount; i++) {
                    builder.add(rowExpression("bigint" + i + " + 5"));
                }
            }
            else if (type == VARCHAR) {
                for (int i = 0; i < columnCount; i++) {
                    // alternatively use identity expression rowExpression("varchar" + i, type) or
                    // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                    builder.add(rowExpression("concat(varchar" + i + ", 'foo')"));
                }
            }
            return builder.build();
        }

        private RowExpression rowExpression(String value)
        {
            Expression expression = createExpression(value, METADATA, TypeProvider.copyOf(symbolTypes));

            return SqlToRowExpressionTranslator.translate(
                    expression,
                    SCALAR,
                    TYPE_ANALYZER.getTypes(TEST_SESSION, TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    METADATA,
                    TEST_SESSION,
                    true);
        }

        private static Page createPage(List<? extends Type> types, int positions, boolean dictionary)
        {
            if (dictionary) {
                return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, positions);
            }
            else {
                return SequencePageBuilder.createSequencePage(types, positions);
            }
        }
    }


    @Benchmark
    public List<Page> benchmarkColumnOriented(BenchmarkScanFilterAndProjectOmniOperator.Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        SourceOperator operator = (SourceOperator) context.getOperatorFactory().createOperator(driverContext);

        operator.addSplit(new Split(new CatalogName("test"), createLocalSplit(), Lifespan.taskWide()));
        operator.setNoMoreSplits();

        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPage.close();
            }
        }
        return null;
    }

    @Test
    public void testBenchmark()
    {
        BenchmarkScanFilterAndProjectOmniOperator.Context context = new BenchmarkScanFilterAndProjectOmniOperator.Context();
        context.setup();
        benchmarkColumnOriented(context);

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkScanFilterAndProjectOmniOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
