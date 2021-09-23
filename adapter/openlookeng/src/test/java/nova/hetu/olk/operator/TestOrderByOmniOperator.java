/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import io.prestosql.operator.DriverContext;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.OperatorUtils;

import nova.hetu.omniruntime.vector.VecAllocator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestOrderByOmniOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMultipleOutputPages()
    {
        // make operator produce multiple pages during finish phase
        long start = System.nanoTime();
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OrderByOmniOperator.OrderByOmniOperatorFactory operatorFactory = createOrderByOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        DriverContext driverContext = createDriverContext(0);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, offHeapPages, false);

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
        long end = System.nanoTime();
        double cost = ((double) (end - start)) / 1000000.0;
        System.out.println("testMultipleOutputPages elapsed time : " + cost + "ms");
    }

    @Test
    public void testSingleFieldKey()
    {
        long start = System.nanoTime();
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        OrderByOmniOperator.OrderByOmniOperatorFactory operatorFactory = createOrderByOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, ASC_NULLS_LAST));

        DriverContext driverContext = createDriverContext(0);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapPages, expected, false);

        long end = System.nanoTime();
        double cost = ((double) (end - start)) / 1000000.0;
        System.out.println("testSingleFieldKey elapsed time : " + cost + "ms");
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
