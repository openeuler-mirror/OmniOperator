/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DummySpillerFactory;
import io.prestosql.operator.HashAggregationOperator;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Test(singleThreaded = true)
public class TestHashAggregationOmniOperator
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getAggregateFunctionImplementation(
            new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
            new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private List<Page> builderPage()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);

        List<Page> inputPages = new ArrayList<>();
        for (int k = 0; k < totalPageCount; k++) {
            PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
            BlockBuilder group1 = pb.getBlockBuilder(0);
            BlockBuilder group2 = pb.getBlockBuilder(1);
            BlockBuilder sum1 = pb.getBlockBuilder(2);
            BlockBuilder sum2 = pb.getBlockBuilder(3);

            for (int i = 0; i < pageDistinctCount; i++) {
                for (int j = 0; j < pageDistinctValueRepeatCount; j++) {
                    group1.writeLong(i);
                    group2.writeLong(i);
                    sum1.writeLong(1);
                    sum2.writeLong(1);
                    pb.declarePosition();
                }
            }
            Page build = pb.build();

            inputPages.add(build);
        }
        return inputPages;
    }

    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 250;
    int totalPageCount = 10;
    int threadNum = 10;

    @Test(invocationCount = 1)
    public void testHashAggregation()
    {
        int[] omniGrouByChannels = {0, 1};
        VecType[] omniGroupByTypes = {LongVecType.LONG, LongVecType.LONG};
        int[] omniAggregationChannels = {2, 3};
        VecType[] omniAggregationTypes = {LongVecType.LONG, LongVecType.LONG};
        AggType[] omniAggregator = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        VecType[] omniAggReturnTypes = {LongVecType.LONG, LongVecType.LONG};
        List<VecType[]> inAndOutputTypes = new ArrayList<>();
        inAndOutputTypes.add(new VecType[] {LongVecType.LONG, LongVecType.LONG, LongVecType.LONG, LongVecType.LONG});
        inAndOutputTypes.add(new VecType[] {LongVecType.LONG, LongVecType.LONG, LongVecType.LONG, LongVecType.LONG});
        int[] outputLayout = new int[] {0, 1, 2, 3};

//        expected
        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult expected = getExpectedMaterializedRows(driverContext);

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        CopyOnWriteArrayList<List<Page>> resultList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadNum; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    List<Page> input = builderPage();
                    List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(input);
                    List<Page> pages;

                    HashAggregationOmniOperator.HashAggregationOmniOperatorFactory hashAggregationOmniOperatorFactory = new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(id, new PlanNodeId(String.valueOf(id)), omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, inAndOutputTypes);
                    pages = toPages(hashAggregationOmniOperatorFactory, driverContext, offHeapPages, false);
                    resultList.add(pages);
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(resultList.size(), threadNum);
        for (List<Page> pages : resultList) {
            assertPagesEqualIgnoreOrder(driverContext, pages, expected, false, Optional.empty());
        }
    }

    private MaterializedResult getExpectedMaterializedRows(DriverContext driverContext)
    {
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT);
        long sum = totalPageCount * pageDistinctValueRepeatCount;
        for (int i = 0; i < pageDistinctCount; i++) {
            expectedBuilder.row((long) i, (long) i, sum, sum);
        }
        MaterializedResult expected = expectedBuilder.build();
        return expected;
    }

    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler(createTestMetadataManager());

    private HashAggregationOperator.HashAggregationOperatorFactory getOriginalAggFactory(int id)
    {
        InternalAggregationFunction bigintSum = metadata.getAggregateFunctionImplementation(
                new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        HashAggregationOperator.HashAggregationOperatorFactory aggregationOperatorFactory = new HashAggregationOperator.HashAggregationOperatorFactory(
                id,
                new PlanNodeId(String.valueOf(id)),
                ImmutableList.of(BIGINT, BIGINT),
                Ints.asList(0, 1),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                ImmutableList.of(bigintSum.bind(ImmutableList.of(2), Optional.empty()), bigintSum.bind(ImmutableList.of(3), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                JOIN_COMPILER,
                false);
        return aggregationOperatorFactory;
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @Test(invocationCount = 1)
    public void testHashAggregationWithDiffLayout()
    {
        int[] omniGrouByChannels = {3, 0};
        VecType[] omniGroupByTypes = {LongVecType.LONG, LongVecType.LONG};
        int[] omniAggregationChannels = {2, 1};
        VecType[] omniAggregationTypes = {LongVecType.LONG, LongVecType.LONG};
        AggType[] omniAggregator = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
        VecType[] omniAggReturnTypes = {LongVecType.LONG, LongVecType.LONG};
        List<VecType[]> inAndOutputTypes = new ArrayList<>();
        inAndOutputTypes.add(new VecType[] {LongVecType.LONG, LongVecType.LONG, LongVecType.LONG, LongVecType.LONG});
        inAndOutputTypes.add(new VecType[] {LongVecType.LONG, LongVecType.LONG, LongVecType.LONG, LongVecType.LONG});
        int[] outputLayout = new int[] {3, 0, 2, 1};

        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT);
        long sum = totalPageCount * pageDistinctValueRepeatCount * 10;
        for (int i = 0; i < pageDistinctCount; i++) {
            expectedBuilder.row((long) i + 1, (long) i, sum, sum);
        }
        MaterializedResult expected = expectedBuilder.build();

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        CopyOnWriteArrayList<List<Page>> resultList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadNum; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    List<Page> input = builderPageWithDiffLayout();
                    List<Page> offHeapPages = OperatorUtils.transferToOffHeapPages(input);
                    List<Page> pages;

                    HashAggregationOmniOperator.HashAggregationOmniOperatorFactory hashAggregationOmniOperatorFactory = new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(id, new PlanNodeId(String.valueOf(id)), omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, inAndOutputTypes);
                    pages = toPages(hashAggregationOmniOperatorFactory, driverContext, offHeapPages, false);
                    resultList.add(pages);
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(resultList.size(), threadNum);
        for (List<Page> pages : resultList) {
            assertPagesEqualIgnoreOrder(driverContext, pages, expected, false, Optional.empty());
        }
    }

    private List<Page> builderPageWithDiffLayout()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);

        List<Page> inputPages = new ArrayList<>();
        for (int k = 0; k < totalPageCount; k++) {
            PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
            BlockBuilder group1 = pb.getBlockBuilder(1);
            BlockBuilder group2 = pb.getBlockBuilder(0);
            BlockBuilder sum1 = pb.getBlockBuilder(2);
            BlockBuilder sum2 = pb.getBlockBuilder(3);

            for (int i = 0; i < pageDistinctCount; i++) {
                for (int j = 0; j < pageDistinctValueRepeatCount; j++) {
                    group1.writeLong(i);
                    group2.writeLong(i + 1);
                    sum1.writeLong(10);
                    sum2.writeLong(10);
                    pb.declarePosition();
                }
            }
            Page build = pb.build();

            inputPages.add(build);
        }
        return inputPages;
    }
}
