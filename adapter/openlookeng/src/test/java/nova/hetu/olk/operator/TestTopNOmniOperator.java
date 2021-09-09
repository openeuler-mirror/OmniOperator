/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableList;

import io.airlift.units.DataSize;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestTopNOmniOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;


    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testSingleFieldKey()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();
        //transfer on-heap page to off-heap
        List<Page> offHeapInput = transferToOffHeapPages(input);

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(6L, 0.6)
                .row(5L, 0.5)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test(enabled = false)
    public void testSingleFieldKeyDictionaryVec()
    {

        int[] ints={2,1,4,3,2};
        IntArrayOmniBlock intArrayOmniBlock = new IntArrayOmniBlock(ints.length, Optional.empty(),ints);
        int[] ids = {0, 1, 2, 3, 4};
        DictionaryOmniBlock<Integer> integerDictionaryOmniBlock = new DictionaryOmniBlock<Integer>(intArrayOmniBlock,ids);
        Page input = new Page(integerDictionaryOmniBlock);

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
            0,
            new PlanNodeId("test"),
            ImmutableList.of(BIGINT, DOUBLE),
            2,
            ImmutableList.of(0),
            ImmutableList.of(DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
            .row(6L, 0.6)
            .row(5L, 0.5)
            .build();

        assertOperatorEquals(operatorFactory, driverContext, Collections.singletonList(input), expected);
    }

    @Test
    public void testMultiFieldKey()
    {
        List<Page> input = rowPagesBuilder(INTEGER, BIGINT)
                .row(0, 1L)
                .row(1, 2L)
                .pageBreak()
                .row(5, 3L)
                .row(0, 4L)
                .pageBreak()
                .row(3, 5L)
                .row(3, 7L)
                .row(4, 6L)
                .build();

        List<Page> offHeapInput = transferToOffHeapPages(input);

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), INTEGER, BIGINT)
                .row(5, 3L)
                .row(4, 6L)
                .row(3, 7L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test(enabled = false)
    public void testMultiFieldKeyWithVarChar()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("f", 3L)
                .row("a", 4L)
                .pageBreak()
                .row("d", 5L)
                .row("d", 7L)
                .row("e", 6L)
                .build();

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("f", 3L)
                .row("e", 6L)
                .row("d", 7L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testReverseOrder()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();
        List<Page> offHeapInput = transferToOffHeapPages(input);

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(-1L, -0.1)
                .row(1L, 0.1)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, offHeapInput, expected);
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                0,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertNull(operator.getOutput());
            assertFalse(operator.needsInput());
            assertNull(operator.getOutput());
        }
    }

    @Test(enabled = false)
    public void testExceedMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .build();

        DriverContext smallDiverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(1, BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));
        try (Operator operator = operatorFactory.createOperator(smallDiverContext)) {
            operator.addInput(input.get(0));
            operator.getOutput();
            fail("must fail because of exceeding local memory limit");
        }
        catch (ExceededMemoryLimitException ignore) {
        }
    }
}
