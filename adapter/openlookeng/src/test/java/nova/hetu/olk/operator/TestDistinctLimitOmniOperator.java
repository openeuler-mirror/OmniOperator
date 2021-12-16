/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;

@Test(singleThreaded = true)
public class TestDistinctLimitOmniOperator {
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp() {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(enabled = true)
    public void testDistinctLimitColTypesCover() {
        List<Page> input = rowPagesBuilder(BIGINT, INTEGER, createCharType(10), INTEGER, DOUBLE, BOOLEAN, VARCHAR,
                createDecimalType(10, 2)).row(1000L, 3, "aaa", 0, 6.6, true, "hello", 1001)
                        .row(2000L, 4, "bbb", 1, 5.5, false, "world", 2002).pageBreak()
                        .row(1000L, 5, "aaa", 0, 6.6, true, "hello", 1001)
                        .row(3000L, 6, "ccc", 2, 4.4, false, "welcome", 3003).build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(BIGINT, INTEGER, createCharType(10), INTEGER, DOUBLE,
                        BOOLEAN, VARCHAR, createDecimalType(10, 2)),
                ImmutableList.of(2, 3, 4, 5, 6, 7), Optional.of(0), 100);

        List<Type> expectedTypes = ImmutableList.of(createCharType(10), INTEGER, DOUBLE, BOOLEAN, VARCHAR,
                createDecimalType(10, 2), BIGINT);
        List<Page> output = rowPagesBuilder(createCharType(10), INTEGER, DOUBLE, BOOLEAN, VARCHAR,
                createDecimalType(10, 2), BIGINT).row("aaa", 0, 6.6, true, "hello", 1001, 1000L)
                        .row("bbb", 1, 5.5, false, "world", 2002, 2000L)
                        .row("ccc", 2, 4.4, false, "welcome", 3003, 3000L).build();
        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), expectedTypes, output);

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test(enabled = true)
    public void testDistinctLimitBasic() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE).row(1L, 0.1).row(2L, 0.2).pageBreak().row(1L, 0.1)
                .row(-1L, -0.1).row(4L, 0.4).pageBreak().row(5L, 0.5).row(4L, 0.41).row(6L, 0.6).pageBreak().build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(0,
                new PlanNodeId("test"), ImmutableList.of(BIGINT, DOUBLE), ImmutableList.of(0, 1), Optional.empty(), 3);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE).row(1L, 0.1)
                .row(2L, 0.2).row(-1L, -0.1).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test(enabled = true)
    public void testDistinctLimitHasNull() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE).row(1L, 0.1).row(2L, 0.2).pageBreak().row(null, null)
                .row(1L, 0.1).row(4L, 0.4).pageBreak().row(5L, 0.5).row(4L, 0.41).row(6L, 0.6).pageBreak().build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(0,
                new PlanNodeId("test"), ImmutableList.of(BIGINT, DOUBLE), ImmutableList.of(0, 1), Optional.empty(), 4);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE).row(1L, 0.1)
                .row(2L, 0.2).row(null, null).row(4L, 0.4).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test(enabled = true)
    public void testDistinctLimitHashCol() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE, BIGINT).row(1L, 0.1, 10L).row(2L, 0.2, 20L).pageBreak()
                .row(1L, 0.1, 10L).row(-1L, -0.1, -10L).row(4L, 0.4, 40L).pageBreak().row(5L, 0.5, 50L)
                .row(4L, 0.41, 60L).row(6L, 0.6, 70L).pageBreak().build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(0,
                new PlanNodeId("test"), ImmutableList.of(BIGINT, DOUBLE, BIGINT), ImmutableList.of(0, 1),
                Optional.of(2), 3);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT)
                .row(1L, 0.1, 10L).row(2L, 0.2, 20L).row(-1L, -0.1, -10L).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }
}
