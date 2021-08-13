/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.operator.WorkProcessorAssertion.assertFinishes;
import static io.prestosql.operator.WorkProcessorAssertion.validateResult;
import static io.prestosql.operator.project.MergePages.mergePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.stream;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import io.prestosql.Session;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.filterandproject.OmniMergePages;

import org.testng.annotations.Test;

import java.util.List;

public class TestMergePagesOmni
{
    private static final List<Type> TYPES = ImmutableList.of(DATE, DOUBLE, BIGINT);


    public static final Session TEST_SESSION = testSessionBuilder()
        .setCatalog("tpch")
        .setSchema("tiny")
        .setClientCapabilities(stream(ClientCapabilities.values())
            .map(ClientCapabilities::toString)
            .collect(toImmutableSet()))
        .setSystemProperty("extension_execution_planner_enabled", "true")
        .setSystemProperty("extension_execution_planner_jar_path", "file:///hong/omni-runtime/omni-openLooKeng-adapter/target/omni-openLooKeng-adapter-1.2.0-SNAPSHOT.jar")
        .setSystemProperty("extension_execution_planner_class_path", "nova.hetu.olk.OmniLocalExecutionPlanner")
        .setSystemProperty("extension_merge_pages_class_path", "nova.hetu.olk.operator.filterandproject.OmniMergePages")
        .setSystemProperty("optimize_hash_generation", "false")
        .build();

    @Test
    public void testMinPageSizeThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes(),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                pagesSource(page),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMinRowCountThreshold()
    {
        Page page = createSequencePage(TYPES, 20);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                1024 * 1024,
                page.getPositionCount(),
                Integer.MAX_VALUE,
                pagesSource(page),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testBufferSmallPages()
    {
        int singlePageRowCount = 10;
        Page page = createSequencePage(TYPES, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes() + 1,
                page.getPositionCount() + 1,
                Integer.MAX_VALUE,
                pagesSource(splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnBigPage()
    {
        Page smallPage = createSequencePage(TYPES, 20);
        Page bigPage = createSequencePage(TYPES, 100);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                bigPage.getSizeInBytes(),
                bigPage.getPositionCount(),
                Integer.MAX_VALUE,
                pagesSource(smallPage, bigPage),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, smallPage));
        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, bigPage));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnFullPage()
    {
        int singlePageRowCount = 50;
        List<Type> types = ImmutableList.of(BIGINT);
        Page page = createSequencePage(types, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                types,
                page.getSizeInBytes() / 2 + 1,
                page.getPositionCount() / 2 + 1,
                toIntExact(page.getSizeInBytes()),
                pagesSource(splits.get(0), splits.get(1), splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMergeOnLargePage()
    {
        int singlePageRowCount = 10;
        Page page = createSequencePage(TYPES, singlePageRowCount * 3);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 3);
        System.out.println("Updated");
        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes() / 3 + 1,
                page.getPositionCount() / 3 + 1,
                pagesSource(splits),
                newSimpleAggregatedMemoryContext(),
                TEST_SESSION);

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testPageToVector()
    {
        Page page = createSequencePage(TYPES, 10);
        Page page1 = createSequencePage(TYPES, 20);
        Page page2 = createSequencePage(TYPES, 30);


        OmniMergePages omni = new OmniMergePages(TYPES,5000,1024, 1024 * 1024, newSimpleAggregatedMemoryContext().newLocalMemoryContext(OmniMergePages.class.getSimpleName()));
        omni.appendPage(page);
        omni.appendPage(page1);
        omni.appendPage(page2);
        Page actualPage = omni.getOutput();
        assertEquals(actualPage.getPositionCount(), 60);
    }

    private static WorkProcessor<Page> pagesSource(Page... pages)
    {
        return WorkProcessor.fromIterable(ImmutableList.copyOf(pages));
    }

    private static WorkProcessor<Page> pagesSource(List<Page> pages)
    {
        return WorkProcessor.fromIterable(ImmutableList.copyOf(pages));
    }
}
