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
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.*;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
import nova.hetu.olk.operator.filterandproject.FilterAndProjectOmniOperator;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.FunctionAssertions.createExpression;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.DateType.DATE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFilterAndProjectOmniOperator {
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final TypeAnalyzer TYPE_ANALYZER = new TypeAnalyzer(new SqlParser(), METADATA);

    private static final int TOTAL_POSITIONS = 1_000_000;
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = new DataSize(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    private static List<Page> pages = new ArrayList<>();

    private static final Map<String, ImmutableList<Type>> INPUT_TYPES =
            ImmutableMap.<String, ImmutableList<Type>>builder()
                    .put("q1", ImmutableList.of(INTEGER, INTEGER, INTEGER, DATE))
                    .put("q2", ImmutableList.of(BIGINT, BIGINT, INTEGER, INTEGER))
                    .put("q3", ImmutableList.of(VARCHAR, VARCHAR, INTEGER))
                    .put("q4", ImmutableList.of(VARCHAR, INTEGER, INTEGER))
                    .put("q5", ImmutableList.of(VARCHAR, INTEGER, INTEGER))
                    .put("q6", ImmutableList.of(VARCHAR, BIGINT, INTEGER))
                    .put("q7", ImmutableList.of(VARCHAR, INTEGER))
                    .put("q8", ImmutableList.of(VARCHAR, VARCHAR, BIGINT, INTEGER))
                    .put("q9", ImmutableList.of(BIGINT, INTEGER, INTEGER, VARCHAR))
                    .put("q10", ImmutableList.of(BIGINT, INTEGER, INTEGER, VARCHAR))
                    .build();

    @State(Thread)
    public static class Context {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        @Param({"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"})
        String query;

        @Param({"32", "1024"})
        int positionsPerPage = 32;

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Setup
        public void setup() {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            List<Type> types = INPUT_TYPES.get(this.query);

            for (int i = 0; i < types.size(); i++) {
                Symbol symbol = new Symbol(types.get(i).getDisplayName().toLowerCase(ENGLISH) + i);
                symbolTypes.put(symbol, types.get(i));
                sourceLayout.put(symbol, i);
            }

            List<RowExpression> projections = getProjections();

            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(METADATA, 0);

            PageProcessor pageProcessor = new OmniExpressionCompiler(METADATA, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter()), projections, Optional.empty(),
                    OptionalInt.empty(), types, new TaskId("test")).get();

            createTaskContext();
            pages = createInputPages(types);
            createScanFilterAndProjectOperatorFactories(pageProcessor, types);
        }

        @TearDown
        public void cleanup() {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private void createScanFilterAndProjectOperatorFactories(PageProcessor pageProcessor, List<Type> types) {
            operatorFactory = new FilterAndProjectOmniOperator.FilterAndProjectOmniOperatorFactory(0, new PlanNodeId("test"),
                    () -> pageProcessor, types, FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE, FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT);

        }

        public TaskContext createTaskContext() {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory() {
            return operatorFactory;
        }

        private List<Page> createInputPages(List<Type> types) {
            List<Page> pages = new ArrayList<>();
            for (int i = 0; i < TOTAL_POSITIONS / positionsPerPage; i++) {
                if (dictionaryBlocks) {
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(types, positionsPerPage));
                } else {
                    pages.add(PageBuilderUtil.createSequencePage(types, positionsPerPage));
                }
            }
            return OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
        }


        private RowExpression getFilter() {
            switch(query) {
                case "q1":
                    return rowExpression("integer0 in (1,2) or (integer1 between 1 and 10) or integer2 in (0,1,2,3)");
                case "q2":
                    return rowExpression("bigint0 > 0 or integer2 = 10 or (integer3 in (1,2) or integer3 = 3)");
                case "q3":
                    return rowExpression("varchar0 in ('1','2','3','4','5','6','7','8','9','10') or varchar1 in ('1','2') or integer2 in (1,2,3,4,5)");
                case "q4":
                    return rowExpression("varchar0 in ('1','2','3') or integer1 = 3 or integer2 = 3");
                case "q5":
                    return rowExpression("varchar0 = '3' or integer1 >= 10 or integer2 <= 20");
                case "q6":
                    return rowExpression("varchar0 in ('1','2','3','4','5','6','7','8','9','10') or (bigint1 between 1 and 10) or integer2 in (1,2,3,4,5)");
                case "q7":
                    return rowExpression("integer1 between 3 and 5");
                case "q8":
                    return rowExpression("varchar0 in ('1','2','3','4','5','6','7','8','9','10') or bigint2 between 3 and 5 or integer3 = 3");
                case "q9":
                    return rowExpression("bigint0 between 3 and 5 or integer1 = 3 or integer2 in (1,2)");
                case "q10":
                    return rowExpression("bigint0 between 3 and 5 or integer1 = 3 or integer2 = 3");
                default:
                    throw new IllegalArgumentException("Unsupported query!");
            }
        }

        private List<RowExpression> getProjections() {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();

            switch (query) {
                case "q1":
                {
                    builder.add(rowExpression("integer0"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("date3"));
                    break;
                }
                case "q2":
                {
                    builder.add(rowExpression("bigint0 - 1"));
                    builder.add(rowExpression("bigint1 + 1"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("cast(integer3 as BIGINT)"));
                    break;
                }
                case "q3":
                {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("varchar1"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    break;
                }
                case "q4":
                {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    break;
                }
                case "q5":
                {
                    builder.add(rowExpression("concat(concat('foo', varchar0), 'lish')"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    break;
                }
                case "q6":
                {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("bigint1"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    break;
                }
                case "q7":
                {
                    builder.add(rowExpression("substr(varchar0, 1, 1)"));
                    builder.add(rowExpression("integer1"));
                    break;
                }
                case "q8":
                {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("varchar1"));
                    builder.add(rowExpression("bigint2"));
                    builder.add(rowExpression("cast(integer3 as BIGINT)"));
                    break;
                }
                case "q9":
                {
                    builder.add(rowExpression("bigint0"));
                    builder.add(rowExpression("cast(integer1 as BIGINT)"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    builder.add(rowExpression("substr(varchar3, 1, 1)"));
                    break;
                }
                case "q10":
                {
                    builder.add(rowExpression("bigint0"));
                    builder.add(rowExpression("cast(integer1 as BIGINT)"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("substr(varchar3, 1, 1)"));
                    break;
                }
                default:
                    break;
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
    public List<Page> benchmark(BenchmarkFilterAndProjectOmniOperator.Context context)
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

        List<Page> pages = outputPages.build();
        for (Page page : pages) {
            page.close();
        }
        return pages;

    }


    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkFilterAndProjectOmniOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
