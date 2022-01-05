package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestAggregationOmniOperator {
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getAggregateFunctionImplementation(
        new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));

    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
        new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
        new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    InternalAggregationFunction maxVarcharColumn = metadata.getAggregateFunctionImplementation(
        new Signature("max", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR),
            parseTypeSignature(StandardTypes.VARCHAR)));

    private SymbolAllocator symbolAllocator;

    private Symbol columnA;

    private Symbol columnB;

    private Symbol columnC;

    private Symbol columnD;

    private ExecutorService executor;

    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp() {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", DOUBLE);
        columnC = symbolAllocator.newSymbol("c", BIGINT);
        columnD = symbolAllocator.newSymbol("d", VARCHAR);

    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled() {
        return new Object[][] {{true}, {false}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(invocationCount = 1)
    public void testAggregation() {

        List<Page> input = rowPagesBuilder(BIGINT, BIGINT, BIGINT, VARCHAR).addSequencePage(100, 0, 0, 0, 300).build();

        AggregationNode.Aggregation aggregation1 = new AggregationNode.Aggregation(
            new Signature("count", FunctionKind.AGGREGATE, ImmutableList.of(), ImmutableList.of(),
                BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature()), false),
            ImmutableList.of(columnA.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty());

        AggregationNode.Aggregation aggregation2 = new AggregationNode.Aggregation(
            new Signature("avg", FunctionKind.AGGREGATE, ImmutableList.of(), ImmutableList.of(),
                DOUBLE.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature()), false),
            ImmutableList.of(columnB.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty());

        AggregationNode.Aggregation aggregation3 = new AggregationNode.Aggregation(
            new Signature("sum", FunctionKind.AGGREGATE, ImmutableList.of(), ImmutableList.of(),
                BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature()), false),
            ImmutableList.of(columnC.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty());

        AggregationNode.Aggregation aggregation4 = new AggregationNode.Aggregation(
            new Signature("max", FunctionKind.AGGREGATE, ImmutableList.of(), ImmutableList.of(),
                VARCHAR.getTypeSignature(), ImmutableList.of(VARCHAR.getTypeSignature()), false),
            ImmutableList.of(columnD.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty());

        ImmutableList<AggregationNode.Aggregation> aggregations = ImmutableList.of(aggregation1, aggregation2,
            aggregation3, aggregation4);
        ImmutableList<AccumulatorFactory> accumulatorFactories = ImmutableList.of(
            COUNT.bind(ImmutableList.of(0), Optional.empty()), LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty()),
            LONG_SUM.bind(ImmutableList.of(2), Optional.empty()),
            maxVarcharColumn.bind(ImmutableList.of(3), Optional.empty()));

        AggregationNode.Step step = AggregationNode.Step.PARTIAL;

        int id = 0;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT, VARCHAR);
        AggregationOmniOperator.AggregationOmniOperatorFactory AggregationOmniOperatorFactory
            = new AggregationOmniOperator.AggregationOmniOperatorFactory(id, new PlanNodeId(String.valueOf(id)), types,
            aggregations, accumulatorFactories, step);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION).addPipelineContext(0,
            true, true, false).addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, VARCHAR).row(
            100L, 49.5, 4950L, "399").build();
        assertOperatorEquals(AggregationOmniOperatorFactory, driverContext, input, expected);
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }
}
