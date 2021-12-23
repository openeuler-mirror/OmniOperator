/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSink;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorIsBlocked;
import static io.prestosql.operator.OperatorAssertion.assertOperatorIsUnblocked;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_FIRST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLocalMergeSourceOmniOperator {
    private AtomicInteger operatorId = new AtomicInteger();

    private ScheduledExecutorService executor;
    private LocalExchange.LocalExchangeFactory localExchangeFactory;
    private OrderingCompiler orderingCompiler;

    @BeforeMethod
    public void setUp() {
        executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-local-merge-source-omni-operator-%s"));
        orderingCompiler = new OrderingCompiler();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        orderingCompiler = null;

        executor.shutdownNow();
        executor = null;

        localExchangeFactory = null;
    }

    @Test
    public void testSingleStream() throws Exception {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        int defaultConcurrency = 2;
        PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = PipelineExecutionStrategy.UNGROUPED_EXECUTION;

        localExchangeFactory = new LocalExchange.LocalExchangeFactory(FIXED_PASSTHROUGH_DISTRIBUTION, defaultConcurrency,
                types, ImmutableList.of(), Optional.empty(),
                exchangeSourcePipelineExecutionStrategy, new DataSize(32, DataSize.Unit.MEGABYTE));
        LocalExchange.LocalExchangeSinkFactoryId sinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();

        LocalExchange localExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());
        LocalExchange.LocalExchangeSinkFactory sinkFactory = localExchange.getSinkFactory(sinkFactoryId);

        List<LocalExchangeSink> sinkList = new ArrayList<>();
        for (int i = 0; i < defaultConcurrency; i++) {
            sinkList.add(sinkFactory.createSink());
        }

        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalMergeSourceOmniOperator operator = createLocalMergeSourceOmniOperator(types, ImmutableList.of(1),
                ImmutableList.of(0, 1), ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST), driverContext);

        List<Page> input = rowPagesBuilder(types).row(1, 1).row(2, 2).pageBreak().row(3, 3).row(4, 4).build();

        assertFalse(operator.isFinished());
        assertOperatorIsBlocked(operator);
        sinkList.get(0).addPage(input.get(0));
        assertOperatorIsUnblocked(operator);

        sinkList.get(1).addPage(input.get(1));
        assertOperatorIsUnblocked(operator);

        assertNull(operator.getOutput());
        sinkFinsh(sinkList);
        assertOperatorIsUnblocked(operator);

        Page expected = rowPagesBuilder(BIGINT).row(1).row(2).row(3).row(4).build().get(0);
        assertPageEquals(ImmutableList.of(BIGINT), getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    @Test
    public void testMergeDifferentTypes() throws Exception {
        ImmutableList<Type> types = ImmutableList.of(BIGINT, INTEGER);
        int defaultConcurrency = 2;
        PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = PipelineExecutionStrategy.UNGROUPED_EXECUTION;
        localExchangeFactory = new LocalExchange.LocalExchangeFactory(FIXED_PASSTHROUGH_DISTRIBUTION, defaultConcurrency,
                types, ImmutableList.of(), Optional.empty(),
                exchangeSourcePipelineExecutionStrategy, new DataSize(32, DataSize.Unit.MEGABYTE));
        LocalExchange.LocalExchangeSinkFactoryId sinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();

        LocalExchange localExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());
        LocalExchange.LocalExchangeSinkFactory sinkFactory = localExchange.getSinkFactory(sinkFactoryId);

        List<LocalExchangeSink> sinkList = new ArrayList<>();
        for (int i = 0; i < defaultConcurrency; i++) {
            sinkList.add(sinkFactory.createSink());
        }

        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalMergeSourceOmniOperator operator = createLocalMergeSourceOmniOperator(types, ImmutableList.of(1, 0),
                ImmutableList.of(1, 0), ImmutableList.of(DESC_NULLS_FIRST, ASC_NULLS_FIRST), driverContext);

        List<Page> input1 = rowPagesBuilder(types).row(0, null).row(1, 4).row(2, 3).build();
        List<Page> input2 = rowPagesBuilder(types).row(null, 5).row(2, 5).row(4, 3).build();

        assertFalse(operator.isFinished());
        assertOperatorIsBlocked(operator);
        sinkList.get(0).addPage(input1.get(0));
        assertOperatorIsUnblocked(operator);

        sinkList.get(1).addPage(input2.get(0));
        assertOperatorIsUnblocked(operator);

        assertNull(operator.getOutput());
        sinkFinsh(sinkList);
        assertOperatorIsUnblocked(operator);

        ImmutableList<Type> outputTypes = ImmutableList.of(INTEGER, BIGINT);
        Page expected = rowPagesBuilder(outputTypes).row(null, 0).row(5, null).row(5, 2).row(4, 1).row(3, 2).row(3, 4)
                .build().get(0);

        assertPageEquals(outputTypes, getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    @Test
    public void testMultipleStreamsSameOutputColumns() throws Exception {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);
        int defaultConcurrency = 8;
        PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = PipelineExecutionStrategy.UNGROUPED_EXECUTION;
        localExchangeFactory = new LocalExchange.LocalExchangeFactory(FIXED_PASSTHROUGH_DISTRIBUTION, defaultConcurrency,
                types, ImmutableList.of(), Optional.empty(),
                exchangeSourcePipelineExecutionStrategy, new DataSize(32, DataSize.Unit.MEGABYTE));
        LocalExchange.LocalExchangeSinkFactoryId sinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();

        LocalExchange localExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());
        LocalExchange.LocalExchangeSinkFactory sinkFactory = localExchange.getSinkFactory(sinkFactoryId);

        List<LocalExchangeSink> sinkList = new ArrayList<>();
        for (int i = 0; i < defaultConcurrency; i++) {
            sinkList.add(sinkFactory.createSink());
        }

        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalMergeSourceOmniOperator operator = createLocalMergeSourceOmniOperator(types, ImmutableList.of(0, 1, 2),
                ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST), driverContext);
        assertOperatorIsBlocked(operator);

        List<Page> input1 = rowPagesBuilder(types).row(1, 1, 2).row(8, 1, 1).row(19, 1, 3).row(27, 1, 4).row(41, 2, 5)
                .pageBreak().row(55, 1, 2).row(89, 1, 3).row(101, 1, 4).row(202, 1, 3).row(399, 2, 2).pageBreak()
                .row(400, 1, 1).row(401, 1, 7).row(402, 1, 6).build();
        List<Page> input2 = rowPagesBuilder(types).row(2, 1, 2).row(8, 1, 1).row(19, 1, 3).row(25, 1, 4).row(26, 2, 5)
                .pageBreak().row(56, 1, 2).row(66, 1, 3).row(77, 1, 4).row(88, 1, 3).row(99, 2, 2).pageBreak()
                .row(99, 1, 1).row(100, 1, 7).row(100, 1, 6).build();
        List<Page> input3 = rowPagesBuilder(types).row(88, 1, 3).row(89, 1, 3).row(90, 1, 3).row(91, 1, 4).row(92, 2, 5)
                .pageBreak().row(93, 1, 2).row(94, 1, 3).row(95, 1, 4).row(97, 1, 3).row(98, 2, 2).build();

        assertOperatorIsBlocked(operator);

        sinkList.get(0).addPage(input1.get(0));
        sinkList.get(1).addPage(input1.get(1));
        sinkList.get(2).addPage(input1.get(2));
        sinkList.get(3).addPage(input2.get(0));
        sinkList.get(4).addPage(input2.get(1));
        sinkList.get(5).addPage(input2.get(2));
        sinkList.get(6).addPage(input3.get(0));
        sinkList.get(7).addPage(input3.get(1));

        assertOperatorIsUnblocked(operator);
        assertNull(operator.getOutput());
        sinkFinsh(sinkList);
        assertOperatorIsUnblocked(operator);

        Page expected = rowPagesBuilder(types).row(1, 1, 2).row(2, 1, 2).row(8, 1, 1).row(8, 1, 1).row(19, 1, 3)
                .row(19, 1, 3).row(25, 1, 4).row(26, 2, 5).row(27, 1, 4).row(41, 2, 5).row(55, 1, 2).row(56, 1, 2)
                .row(66, 1, 3).row(77, 1, 4).row(88, 1, 3).row(88, 1, 3).row(89, 1, 3).row(89, 1, 3).row(90, 1, 3)
                .row(91, 1, 4).row(92, 2, 5).row(93, 1, 2).row(94, 1, 3).row(95, 1, 4).row(97, 1, 3).row(98, 2, 2)
                .row(99, 2, 2).row(99, 1, 1).row(100, 1, 6).row(100, 1, 7).row(101, 1, 4).row(202, 1, 3).row(399, 2, 2)
                .row(400, 1, 1).row(401, 1, 7).row(402, 1, 6).build().get(0);

        assertPageEquals(types, getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    private LocalMergeSourceOmniOperator createLocalMergeSourceOmniOperator(List<Type> sourceTypes,
            List<Integer> outputChannels, List<Integer> sortChannels, List<SortOrder> sortOrder,
            DriverContext driverContext) {
        int mergeOperatorId = operatorId.getAndIncrement();
        int orderByOmniId = mergeOperatorId;
        LocalMergeSourceOmniOperator.LocalMergeSourceOmniOperatorFactory factory = new LocalMergeSourceOmniOperator.LocalMergeSourceOmniOperatorFactory(
                mergeOperatorId, orderByOmniId, new PlanNodeId("plan_node_id" + mergeOperatorId), localExchangeFactory,
                sourceTypes, orderingCompiler, sortChannels, sortOrder, outputChannels);

        return (LocalMergeSourceOmniOperator) factory.createOperator(driverContext);
    }

    private static List<Page> pullAvailablePages(Operator operator) throws InterruptedException {
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Page> outputPages = new ArrayList<>();

        assertOperatorIsUnblocked(operator);

        while (!operator.isFinished() && System.nanoTime() - endTime < 0) {
            assertFalse(operator.needsInput());
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            } else {
                Thread.sleep(10);
            }
        }

        // verify state
        assertFalse(operator.needsInput(), "Operator still wants input");
        assertTrue(operator.isFinished(), "Expected operator to be finished");

        return outputPages;
    }

    private void sinkFinsh(List<LocalExchangeSink> sinkList) {
        if (sinkList == null) {
            return;
        }

        for (LocalExchangeSink sink : sinkList) {
            sink.finish();
        }
    }
}
