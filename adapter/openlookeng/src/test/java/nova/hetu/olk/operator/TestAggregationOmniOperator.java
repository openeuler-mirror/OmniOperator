
package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_COUNT;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static org.testng.Assert.assertEquals;

import io.prestosql.operator.DriverContext;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;

import nova.hetu.olk.operator.AggregationOmniOperator.AggregationOmniOperatorFactory;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestAggregationOmniOperator {
    private ExecutorService executor;

    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp() {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled() {
        return new Object[][]{{true}, {false}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(invocationCount = 1)
    public void testAggregation() {
        List<Page> input = rowPagesBuilder(BIGINT, BIGINT, BIGINT, VARCHAR).addSequencePage(100, 0, 0, 0, 300).build();

        int id = 0;
        VecType[] sourceTypes = {LongVecType.LONG, LongVecType.LONG, LongVecType.LONG, new VarcharVecType(10)};
        AggType[] aggregatorTypes = {OMNI_AGGREGATION_TYPE_COUNT, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM,
                OMNI_AGGREGATION_TYPE_MAX};
        int[] aggregationInputChannels = {0, 1, 2, 3};
        VecType[] aggregationOutputTypes = {LongVecType.LONG, DoubleVecType.DOUBLE, LongVecType.LONG,
                new VarcharVecType(10)};
        AggregationNode.Step step = AggregationNode.Step.SINGLE;
        AggregationOmniOperatorFactory AggregationOmniOperatorFactory = new AggregationOmniOperatorFactory(id,
                new PlanNodeId(String.valueOf(id)), sourceTypes, aggregatorTypes, aggregationInputChannels,
                aggregationOutputTypes, step);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, VARCHAR)
                .row(100L, 49.5, 4950L, "399").build();
        assertOperatorEquals(AggregationOmniOperatorFactory, driverContext, input, expected);
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }
}
