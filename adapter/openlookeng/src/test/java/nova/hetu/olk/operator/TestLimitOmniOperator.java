/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;

@Test(singleThreaded = true)
public class TestLimitOmniOperator {
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

    @Test
    public void testBasicLimit() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE).row(1L, 0.1).row(2L, 0.2).pageBreak().row(-1L, -0.1)
                .row(4L, 0.4).pageBreak().row(5L, 0.5).row(4L, 0.41).row(6L, 0.6).pageBreak().build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new LimitOmniOperator.LimitOmniOperatorFactory(0, new PlanNodeId("test"), 3);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE).row(1L, 0.1)
                .row(2L, 0.2).row(-1L, -0.1).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test
    public void testLimitNull() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE).row(1L, 0.1).row(null, 0.2).pageBreak().row(-1L, -0.1)
                .row(null, null).pageBreak().row(5L, 0.5).row(4L, 0.41).row(6L, 0.6).pageBreak().build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new LimitOmniOperator.LimitOmniOperatorFactory(0, new PlanNodeId("test"), 5);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE).row(1L, 0.1)
                .row(null, 0.2).row(-1L, -0.1).row(null, null).row(5L, 0.5).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test
    public void testLimitLess() {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE).row(1L, 0.1).row(-1L, -0.1).build();
        // transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OperatorFactory operatorFactory = new LimitOmniOperator.LimitOmniOperatorFactory(0, new PlanNodeId("test"), 5);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE).row(1L, 0.1)
                .row(-1L, -0.1).build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }
}
