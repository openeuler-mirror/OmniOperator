/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.prestosql.sql.tree.WindowFrame.Type.RANGE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import io.prestosql.operator.DriverContext;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.RankFunction;
import io.prestosql.operator.window.ReflectionWindowFunctionSupplier;
import io.prestosql.operator.window.RowNumberFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.OperatorUtils;

import nova.hetu.omniruntime.vector.VecAllocator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestWindowOmniOperator
{
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty());
    public static final List<WindowFunctionDefinition> RANK = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("rank", BIGINT, ImmutableList.of(), RankFunction.class), BIGINT, UNBOUNDED_FRAME));
    public static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.of(), RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME));
    public static final List<WindowFunctionDefinition> RankAndRow_NUMBER = new ImmutableList.Builder<WindowFunctionDefinition>()
            .add(window(new ReflectionWindowFunctionSupplier<>("rank", BIGINT, ImmutableList.of(), RankFunction.class), BIGINT, UNBOUNDED_FRAME))
            .add(window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.of(), RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME))
            .build();
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @Test
    public void testRankPartition()
    {
        List<Page> input = rowPagesBuilder(INTEGER, BIGINT, DOUBLE)
                .row(2, -1L, -0.1)
                .row(1, 2L, 0.3)
                .row(1, 4L, 0.2)
                .pageBreak()
                .row(2, 5L, 0.4)
                .row(1, 6L, 0.1)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        WindowOmniOperator.WindowOmniOperatorFactory operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, BIGINT, DOUBLE),
                Ints.asList(0, 1, 2),
                RANK,
                Ints.asList(0),
                Ints.asList(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10000);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), INTEGER, BIGINT, DOUBLE, BIGINT)
                .row(1, 2L, 0.3, 1L)
                .row(1, 4L, 0.2, 2L)
                .row(1, 6L, 0.1, 3L)
                .row(2, -1L, -0.1, 1L)
                .row(2, 5L, 0.4, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapPages, expected, false);
    }

    @Test
    public void testRankPartitionDiffLayout()
    {
        List<Page> input = rowPagesBuilder(INTEGER, BIGINT, DOUBLE)
                .row(2, -1L, -0.1)
                .row(1, 2L, 0.3)
                .row(1, 4L, 0.2)
                .pageBreak()
                .row(2, 5L, 0.4)
                .row(1, 6L, 0.1)
                .row(1, 4L, 0.2)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        WindowOmniOperator.WindowOmniOperatorFactory operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, BIGINT, DOUBLE),
                Ints.asList(2, 0, 1),
                RANK,
                Ints.asList(0),
                Ints.asList(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10000);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, INTEGER, BIGINT, BIGINT)
                .row(0.3, 1, 2L, 1L)
                .row(0.2, 1, 4L, 2L)
                .row(0.2, 1, 4L, 2L)
                .row(0.1, 1, 6L, 4L)
                .row(-0.1, 2, -1L, 1L)
                .row(0.4, 2, 5L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapPages, expected, false);
    }

    @Test
    public void testRowNumberPartition()
    {
        List<Page> input = rowPagesBuilder(INTEGER, BIGINT, DOUBLE)
                .row(2, -1L, -0.1)
                .row(1, 2L, 0.3)
                .row(1, 4L, 0.2)
                .pageBreak()
                .row(2, 5L, 0.4)
                .row(1, 6L, 0.1)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        WindowOmniOperator.WindowOmniOperatorFactory operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, BIGINT, DOUBLE),
                Ints.asList(0, 2, 1),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10000);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), INTEGER, DOUBLE, BIGINT, BIGINT)
                .row(1, 0.3, 2L, 1L)
                .row(1, 0.2, 4L, 2L)
                .row(1, 0.1, 6L, 3L)
                .row(2, -0.1, -1L, 1L)
                .row(2, 0.4, 5L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapPages, expected, false);
    }

    @Test
    public void testRankAndRowNumberPartition()
    {
        List<Page> input = rowPagesBuilder(INTEGER, BIGINT, DOUBLE)
                .row(2, -1L, -0.1)
                .row(1, 2L, 0.3)
                .row(1, 4L, 0.2)
                .row(1, 4L, 0.2)
                .pageBreak()
                .row(2, 5L, 0.4)
                .row(1, 6L, 0.1)
                .build();
        List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, input);

        WindowOmniOperator.WindowOmniOperatorFactory operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, BIGINT, DOUBLE),
                Ints.asList(0, 2, 1),
                RankAndRow_NUMBER,
                Ints.asList(0),
                Ints.asList(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10000);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), INTEGER, DOUBLE, BIGINT, BIGINT, BIGINT)
                .row(1, 0.3, 2L, 1L, 1L)
                .row(1, 0.2, 4L, 2L, 2L)
                .row(1, 0.2, 4L, 2L, 3L)
                .row(1, 0.1, 6L, 4L, 4L)
                .row(2, -0.1, -1L, 1L, 1L)
                .row(2, 0.4, 5L, 2L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapPages, expected, false);
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Long.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

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
}
