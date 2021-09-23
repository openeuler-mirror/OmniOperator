/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.SessionTestUtils;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkFilterAndProject
{
    private static final int SHIP_DATE = 0;
    private static final int EXTENDED_PRICE = 1;
    private static final int DISCOUNT = 2;
    private static final int QUANTITY = 3;

    private static final Slice MIN_SHIP_DATE = utf8Slice("1994-01-01");
    private static final Slice MAX_SHIP_DATE = utf8Slice("1995-01-01");

    private Page inputPage;
    private List<Page> inputPages;
    private PageProcessor compiledProcessor;
    private PageProcessor omniCompiledProcessor;
    private int pagSize = 1024;
    private int pageCount = 1000;
    private static float selectedRatio = 1f;
    private static final long CONDITION = 10471;
    private static final List<Type> inputTypes = ImmutableList.of(INTEGER, BIGINT, BIGINT, BIGINT);
    private Metadata metadata;

    @Setup
    public void setup()
    {
        inputPages = createInputPages(pagSize, pageCount, selectedRatio, CONDITION);
        metadata = createTestMetadataManager();
        compiledProcessor = getCompiledProcessor();
        omniCompiledProcessor = getOmniCompiledProcessor();
    }

    public void finished()
    {

    }

    private PageProcessor getCompiledProcessor()
    {
        return new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 10_000))
                .compilePageProcessor(Optional.of(FILTER_FOR_Q1_OMNI_FILTER), ImmutableList.of(PROJECT)).get();
    }

    private PageProcessor getOmniCompiledProcessor()
    {
        List<Type> inputTypes = new ArrayList<>(4);
        inputTypes.add(INTEGER);
        inputTypes.add(BIGINT);
        inputTypes.add(BIGINT);
        inputTypes.add(BIGINT);
        return new OmniExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 10_000))
                .compilePageProcessor(Optional.of(FILTER_FOR_Q1_OMNI_FILTER), ImmutableList.of(PROJECT), Optional.empty(),
                    OptionalInt.empty(), inputTypes, new TaskId("test"))
                .get();
    }

    // @Benchmark
    // public Page handCoded()
    // {
    //     PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE));
    //     int count = Tpch1FilterAndProject.process(inputPage, 0, inputPage.getPositionCount(), pageBuilder);
    //     checkState(count == inputPage.getPositionCount());
    //     return pageBuilder.build();
    // }

    @Benchmark
    public List<Optional<Page>> compiledWithoutOmniFilterAndProject()
    {
        return ImmutableList.copyOf(
                compiledProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
    }

    @Benchmark
    public List<Optional<Page>> compiledWithOmniFilterAndProject()
    {
        for (Page input : inputPages) {
            Iterator<Optional<Page>> iterator = omniCompiledProcessor.process(
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    new DriverYieldSignal(),
                    newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                    input);
            while (iterator.hasNext()) {
                Optional<Page> page = iterator.next();
                if (page.isPresent()) {
                    page.get().close();
                }
            }
        }
        return ImmutableList.of();
    }

    private static Map<String, List<String>> result = new HashMap<>();

    private static void multiThreadBenchmark(String tag, List<Runnable> runnables)
            throws InterruptedException
    {
        AtomicLong wallTime = new AtomicLong(0);
        AtomicLong cpuTime = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(runnables.size());
        long totalUsedTimeStart = System.nanoTime();
        for (int i = 0; i < runnables.size(); i++) {
            Runnable runnable = runnables.get(i);
            Thread thread = new Thread(() -> {
                long cpuStart = currentThreadCpuTime();
                long wallStart = System.nanoTime();
                runnable.run();
                wallTime.addAndGet(System.nanoTime() - wallStart);
                cpuTime.addAndGet(currentThreadCpuTime() - cpuStart);
                countDownLatch.countDown();
            });
            thread.start();
        }
        countDownLatch.await();
        String msg = String.format("[%s] selectedRatio=%s,threadCount=%s,totalUsedTime=%s ms,wall time used %s ms,cpu time used %s ms,CPU_TIME/WALL_TIME=%s", tag, selectedRatio, runnables.size(), (System.nanoTime() - totalUsedTimeStart) / 1000_000.0, wallTime.get() / 1000_000.0, cpuTime.get() / 1000_000.0, cpuTime.get() * 1.0 / wallTime.get());
        System.out.println(msg);
        if (result.containsKey(tag)) {
            result.get(tag).add(msg);
        }
        else {
            List<String> tmp = new ArrayList<>();
            tmp.add(msg);
            result.put(tag, tmp);
        }
    }

    private static long currentThreadCpuTime()
    {
        return ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    }

    public static void main(String[] args)
            throws InterruptedException
    {
//        testBenchmarkForDiffSelectedRatio();
        selectedRatio = 0.8f;
        // for (int i = 16; i <= 128; i *= 2) {
        for (int i = 1; i <= 1; i++) {
            List<Runnable> tasks = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                BenchmarkFilterAndProject benchmarkOmniPageProcessor = new BenchmarkFilterAndProject();
                benchmarkOmniPageProcessor.setup();
                tasks.add(() -> {
                    List<Optional<Page>> result = benchmarkOmniPageProcessor.compiledWithOmniFilterAndProject();
                    for (Optional<Page> page : result) {
                        if (page.isPresent()) {
                            page.get().close();
                        }
                    }
                });
            }
            multiThreadBenchmark("compiledWithOmniFilterAndProject", tasks);
            Thread.sleep(999);
        }
        selectedRatio += 0.2f;

        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            List<String> msgs = entry.getValue();
            for (String msg : msgs) {
                System.out.println(msg);
            }
        }
    }

    private static void testBenchmarkForDiffSelectedRatio()
            throws InterruptedException
    {
        selectedRatio = 0.2f;
        while (selectedRatio < 1) {
            for (int i = 1; i <= 128; i *= 2) {
                List<Runnable> tasks = new ArrayList<>();
                for (int j = 0; j < i; j++) {
                    BenchmarkFilterAndProject benchmarkOmniPageProcessor = new BenchmarkFilterAndProject();
                    benchmarkOmniPageProcessor.setup();
                    tasks.add(benchmarkOmniPageProcessor::compiledWithoutOmniFilterAndProject);
                }
                multiThreadBenchmark("compiledWithoutOmniFilterAndProject", tasks);
                Thread.sleep(1000);
            }
            selectedRatio += 0.2f;
        }
    }

    //builder page for where SHIP_DATE<=condition
    private static List<Page> createInputPages(int pageSize, int pageCount, float selectedRatio, long condition)
    {
        List<Page> pageContainer = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {

            pageContainer.add(createInputPages(pageSize, selectedRatio, condition));
        }
        return pageContainer;
    }

    private static Page createInputPages(int pageSize, float selectedRatio, long condition)
    {
        PageBuilder pageBuilder = new PageBuilder(inputTypes);
        for (int j = 0; j < pageSize; j++) {
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(EXTENDED_PRICE), j);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(DISCOUNT), j);

            BIGINT.writeLong(pageBuilder.getBlockBuilder(QUANTITY), j);
        }
        //builder Condition column values
        int selectedCount = (int) (selectedRatio * pageSize);
        for (int j = 0; j < selectedCount; j++) {
            INTEGER.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), condition);
        }
        for (int j = selectedCount; j < pageSize; j++) {
            INTEGER.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), condition + 1);
        }
        return pageBuilder.build();
    }

    private static final class Tpch1FilterAndProject
    {
        public static int process(Page page, int start, int end, PageBuilder pageBuilder)
        {
            Block discountBlock = page.getBlock(DISCOUNT);
            int position = start;
            for (; position < end; position++) {
                // where shipdate >= '1994-01-01'
                //    and shipdate < '1995-01-01'
                //    and discount >= 0.05
                //    and discount <= 0.07
                //    and quantity < 24;
                if (filter(position, discountBlock, page.getBlock(SHIP_DATE), page.getBlock(QUANTITY))) {
                    project(position, pageBuilder, page.getBlock(EXTENDED_PRICE), discountBlock);
                }
            }

            return position;
        }

        private static void project(int position, PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock)
        {
            pageBuilder.declarePosition();
            if (discountBlock.isNull(position) || extendedPriceBlock.isNull(position)) {
                pageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), DOUBLE.getDouble(extendedPriceBlock, position) * DOUBLE.getDouble(discountBlock, position));
            }
        }

        private static boolean filter(int position, Block discountBlock, Block shipDateBlock, Block quantityBlock)
        {
            return !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MAX_SHIP_DATE) < 0;
        }
    }

    // where shipdate <= CONDITION ,Currently OmniFilter Native Codegen just support $operator$LESS_THAN_OR_EQUAL(#0, 10471)
    private static final RowExpression FILTER_FOR_Q1_OMNI_FILTER = call(internalOperator(OperatorType.LESS_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), INTEGER.getTypeSignature(), INTEGER.getTypeSignature()),
            BOOLEAN,
            field(SHIP_DATE, BIGINT),
            constant(CONDITION, BIGINT));

//    private static final RowExpression FILTER_FOR_Q1_OMNI_FILTER = call(OperatorType.LESS_THAN_OR_EQUAL.getFunctionName().toString(), new BuiltInFunctionHandle(internalOperator(OperatorType.LESS_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), INTEGER.getTypeSignature(), INTEGER.getTypeSignature())),
//            BOOLEAN,
//            field(SHIP_DATE, BIGINT),
//            constant(CONDITION, BIGINT));

    private static final RowExpression PROJECT = call(
            internalOperator(OperatorType.MULTIPLY, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
            BIGINT,
            field(EXTENDED_PRICE, BIGINT),
            field(DISCOUNT, BIGINT));

//    private static final RowExpression PROJECT = call(OperatorType.MULTIPLY.getFunctionName().toString(),
//            new BuiltInFunctionHandle(internalOperator(OperatorType.MULTIPLY, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature())),
//            BIGINT,
//            field(EXTENDED_PRICE, BIGINT),
//            field(DISCOUNT, BIGINT));
}
