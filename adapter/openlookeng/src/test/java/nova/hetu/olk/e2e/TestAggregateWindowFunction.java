/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.e2e;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;

import io.prestosql.Session;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.operator.window.AbstractTestWindowFunction;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;

import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAggregateWindowFunction
        extends AbstractTestWindowFunction
{
    public static final String TINY_SCHEMA_NAME = "tiny";

    public static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema(TINY_SCHEMA_NAME)
            .setClientCapabilities(stream(ClientCapabilities.values())
                    .map(ClientCapabilities::toString)
                    .collect(toImmutableSet()))
            .setSystemProperty("extension_execution_planner_enabled", "true")
            .setSystemProperty("extension_execution_planner_jar_path", "file:///opt/lib/omni-openLooKeng-adapter-1.2.0-SNAPSHOT.jar")
            .setSystemProperty("extension_execution_planner_class_path", "nova.hetu.olk.OmniLocalExecutionPlanner")
            .setSystemProperty("extension_merge_pages_class_path", "nova.hetu.olk.operator.filterandproject.OmniMergePages")
            .setSystemProperty("prefer_partial_aggregation", "false")
            .setSystemProperty("optimize_hash_generation", "false")
            .build();

    @Override
    @BeforeClass
    public void initTestWindowFunction()
    {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
    }

    @Override
    protected void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQuery(sql, expected, queryRunner);
    }

    protected void assertWindowQuery1(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQuery1(sql, expected, queryRunner);
    }

    @Test
    public void testCountRows()
    {
        assertWindowQuery("count(*)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(1,0,1L)
                        .row(2,0,1L)
                        .row(3,1,1L)
                        .row(4,0,1L)
                        .row(5,1,1L)
                        .row(6,1,1L)
                        .row(7,0,1L)
                        .row(32,0,1L)
                        .row(33,1,1L)
                        .row(34,0,1L)
                        .build());
    }

    @Test(enabled = false)
    public void testCountRowsOrdered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(3, 1, 1L)
                        .row(5, 1, 2L)
                        .row(6, 1, 3L)
                        .row(33, 1, 4L)
                        .row(1, 0, 1L)
                        .row(2, 0, 2L)
                        .row(4, 0, 3L)
                        .row(7, 0, 4L)
                        .row(32, 0, 5L)
                        .row(34, 0, 6L)
                        .build());
    }

    @Test(enabled = false)
    public void testCountRowsUnordered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(3, 1, 4L)
                        .row(5, 1, 4L)
                        .row(6, 1, 4L)
                        .row(33, 1, 4L)
                        .row(1, 0, 6L)
                        .row(2, 0, 6L)
                        .row(4, 0, 6L)
                        .row(7, 0, 6L)
                        .row(32, 0, 6L)
                        .row(34, 0, 6L)
                        .build());
    }

    @Test
    public void testCountValuesOrdered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(3, 1, 1L)
                        .row(5, 1, 2L)
                        .row(6, 1, 3L)
                        .row(33, 1, 4L)
                        .row(1, 0, 1L)
                        .row(2, 0, 2L)
                        .row(4, 0, 3L)
                        .row(7, 0, 4L)
                        .row(32, 0, 5L)
                        .row(34, 0, 6L)
                        .build());
    }

    @Test
    public void testCountValuesUnordered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(3, 1, 4L)
                        .row(5, 1, 4L)
                        .row(6, 1, 4L)
                        .row(33, 1, 4L)
                        .row(1, 0, 6L)
                        .row(2, 0, 6L)
                        .row(4, 0, 6L)
                        .row(7, 0, 6L)
                        .row(32, 0, 6L)
                        .row(34, 0, 6L)
                        .build());
    }

    @Test(enabled = true)
    public void testSumOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                .row(3, 1, 3L)
                .row(5, 1, 8L)
                .row(6, 1, 14L)
                .row(33, 1, 47L)
                .row(1, 0, 1L)
                .row(2, 0, 3L)
                .row(4, 0, 7L)
                .row(7, 0, 14L)
                .row(32, 0, 46L)
                .row(34, 0, 80L)
                .build();
        // default window frame
        @Language("SQL") String sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery(sql, expected);
    }

    @Test(enabled = true)
    public void testSumUnordered()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT)
                        .row(3, 1, 47L)
                        .row(5, 1, 47L)
                        .row(6, 1, 47L)
                        .row(33, 1, 47L)
                        .row(1, 0, 80L)
                        .row(2, 0, 80L)
                        .row(4, 0, 80L)
                        .row(7, 0, 80L)
                        .row(32, 0, 80L)
                        .row(34, 0, 80L)
                        .build());
    }

    @Test(enabled = true)
    public void testAvgOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, DOUBLE)
                .row(3, 1, 3.0)
                .row(5, 1, 4.0)
                .row(6, 1, 14 / 3.0)
                .row(33, 1, 11.75)
                .row(1, 0, 1.0)
                .row(2, 0, 1.5)
                .row(4, 0, 7 / 3.0)
                .row(7, 0, 3.5)
                .row(32, 0, 9.2)
                .row(34, 0, 80.0 / 6)
                .build();
        // default window frame
        @Language("SQL") String sql = "avg(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery(sql, expected);
    }

    @Test(enabled = true)
    public void testAvgAndSumOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, DOUBLE, BIGINT)
                .row(3, 1, 3.0, 3L)
                .row(5, 1, 4.0, 8L)
                .row(6, 1, 14 / 3.0, 14L)
                .row(33, 1, 11.75, 47L)
                .row(1, 0, 1.0, 1L)
                .row(2, 0, 1.5, 3L)
                .row(4, 0, 7 / 3.0, 7L)
                .row(7, 0, 3.5, 14L)
                .row(32, 0, 9.2, 46L)
                .row(34, 0, 80.0 / 6, 80L)
                .build();
        // default window frame
        @Language("SQL") String sql = "avg(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey),sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery(sql, expected);
    }

    @Test(enabled = true)
    public void testSumInAvgOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, BIGINT, DOUBLE)
                .row(1, 0, 3L, 1.0, 3.0)
                .row(2, 0, 4L, 1.5, 3.5)
                .row(4, 0, 4L, 7 / 3.0, 11 / 3.0)
                .row(7, 0, 7L, 3.5, 4.5)
                .row(32, 0, 32L, 9.2, 10.0)
                .row(34, 0, 34L, 80 / 6.0, 14.0)
                .row(3, 1, 6L, 3.0, 6.0)
                .row(5, 1, 5L, 4.0, 5.5)
                .row(6, 1, 6L, 14 / 3.0, 17 / 3.0)
                .row(33, 1, 66L, 11.75, 20.75)
                .build();
        @Language("SQL") String sql = "sum(orderkey) as sum1,avg(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey),avg(sum(orderkey)) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery1(sql, expected);
    }

    @Test
    public void testMaxOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, INTEGER)
                .row(33, 1, 33)
                .row(6, 1, 33)
                .row(5, 1, 33)
                .row(3, 1, 33)
                .row(34, 0, 34)
                .row(32, 0, 34)
                .row(7, 0, 34)
                .row(4, 0, 34)
                .row(2, 0, 34)
                .row(1, 0, 34)
                .build();
        // default window framel
        @Language("SQL") String sql = "max(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey desc)";
        assertWindowQuery(sql, expected);
    }

    @Test
    public void testMaxUnordered()
    {
        assertWindowQuery("max(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, INTEGER)
                        .row(33, 1, 33)
                        .row(6, 1, 33)
                        .row(5, 1, 33)
                        .row(3, 1, 33)
                        .row(34, 0, 34)
                        .row(32, 0, 34)
                        .row(7, 0, 34)
                        .row(4, 0, 34)
                        .row(2, 0, 34)
                        .row(1, 0, 34)
                        .build());
    }

    @Test
    public void testMinOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, INTEGER, INTEGER)
                .row(3, 1, 3)
                .row(5, 1, 3)
                .row(6, 1, 3)
                .row(33, 1, 3)
                .row(1, 0, 1)
                .row(2, 0, 1)
                .row(4, 0, 1)
                .row(7, 0, 1)
                .row(32, 0, 1)
                .row(34, 0, 1)
                .build();
        // default window frame
        @Language("SQL") String sql = "min(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery(sql, expected);
    }

    @Test
    public void testMinUnordered()
    {
        assertWindowQuery("min(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, INTEGER, INTEGER)
                        .row(5, 1, 3)
                        .row(6, 1, 3)
                        .row(33, 1, 3)
                        .row(1, 0, 1)
                        .row(2, 0, 1)
                        .row(4, 0, 1)
                        .row(7, 0, 1)
                        .row(32, 0, 1)
                        .row(34, 0, 1)
                        .row(3, 1, 3)
                        .build());
    }
}
