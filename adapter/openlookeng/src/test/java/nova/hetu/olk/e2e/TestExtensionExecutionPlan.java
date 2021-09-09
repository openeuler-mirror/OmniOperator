/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.e2e;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.stream;

import com.google.common.collect.ImmutableMap;

import io.prestosql.Session;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.plugin.tpcds.TpcdsConnectorFactory;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;

import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestExtensionExecutionPlan {
    public static final Session TEST_SESSION = testSessionBuilder().setCatalog("tpch")
        .setSchema("tiny")
        .setClientCapabilities(
            stream(ClientCapabilities.values()).map(ClientCapabilities::toString).collect(toImmutableSet()))
        .setSystemProperty("extension_execution_planner_enabled", "true")
        .setSystemProperty("extension_execution_planner_jar_path", "file:///hong/omni-runtime/omni-openLooKeng-adapter/target/omni-openLooKeng-adapter-1.2.0-SNAPSHOT.jar")
        .setSystemProperty("extension_execution_planner_class_path", "nova.hetu.olk.OmniLocalExecutionPlanner")
        .setSystemProperty("extension_merge_pages_class_path", "nova.hetu.olk.operator.filterandproject.OmniMergePages")

        .setSystemProperty("prefer_partial_aggregation", "false")
        .setSystemProperty("optimize_hash_generation", "false")
        .build();

    private static final String VALUES = "" + "SELECT *\n" + "FROM (\n" + "  VALUES\n"
        + "    ( CAST(1 AS BIGINT), 0),\n" + "    ( CAST(3 AS BIGINT), 2),\n" + "    ( CAST(1 as BIGINT), 1),\n"
        + "    ( CAST(2 AS BIGINT), 3),\n" + "    ( CAST(2 AS BIGINT), 2),\n" + "    ( CAST(3 AS BIGINT), 2)\n"
        + ") AS orders (orderkey, orderstatus)";

    protected LocalQueryRunner queryRunner;

    @BeforeClass
    public void init() {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
        queryRunner.createCatalog("tpcds", new TpcdsConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy() {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testTopN() {
        @Language("SQL") String query = format(
            "" + "SELECT orderkey, orderstatus " + "FROM (%s) x  order by orderkey,orderstatus limit 3", VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), resultBuilder(TEST_SESSION, BIGINT, INTEGER).row(1L, 0)
            .row(1L, 1)
            .row(2L, 2)
            .build()
            .getMaterializedRows());
    }

    @Test(enabled = false)
    public void testTopNWithDataTypes() {
        @Language("SQL") String query = "select i_current_price, cast(i_rec_start_date as varchar(20)), i_item_sk,i_brand_id from tpcds.sf1.item order by i_current_price, i_rec_start_date, i_item_sk,i_brand_id limit 2";

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), resultBuilder(TEST_SESSION, DOUBLE,VARCHAR, BIGINT,INTEGER)
            .row(0.09, "1997-10-27", 634L, 1004001)
            .row(0.09, "1997-10-27", 691L, 8007002)
            .build()
            .getMaterializedRows());
    }

    @Test(enabled = false)
    public void testTopNWithSql() {
        @Language("SQL") String query = "select i_returnflag, i_linestatus, sum(i_quantity) as sum_qty, sum(i_discount) as sun_disc from tpch.longlineitem group by i_returnflag, i_linestatus order by i_returnflag, i_linestatus;";

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), resultBuilder(TEST_SESSION, DOUBLE,VARCHAR, BIGINT,INTEGER)
            .row(0.09, "1997-10-27", 634L, 1004001)
            .row(0.09, "1997-10-27", 691L, 8007002)
            .build()
            .getMaterializedRows());
    }

    @Test
    public void testHashAgg() {
        @Language("SQL") String query = format(
            "" + "SELECT orderkey, orderstatus,count(*) " + "FROM (%s) x group by orderkey,orderstatus", VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(),
            resultBuilder(TEST_SESSION, BIGINT, INTEGER, BIGINT).row(1L, 0, 1L)
                .row(1L, 1, 1L)
                .row(2L, 2, 1L)
                .row(2L, 3, 1L)
                .row(3L, 2, 2L)
                .build()
                .getMaterializedRows());
    }

    @Test
    public void testOrderBy() {
        @Language("SQL") String query = format(
            "" + "SELECT orderkey, orderstatus " + "FROM (%s) x order by orderkey,orderstatus", VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(),
            resultBuilder(TEST_SESSION, BIGINT, INTEGER, BIGINT).row(1L, 0)
                .row(1L, 1)
                .row(2L, 2)
                .row(2L, 3)
                .row(3L, 2)
                .row(3L, 2)
                .build()
                .getMaterializedRows());
    }

    @Test
    public void testFilterAndProject() {
        @Language("SQL") String query = format("" + "SELECT orderkey, orderstatus " + "FROM (%s) x where orderkey=1",
            VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(),
            resultBuilder(TEST_SESSION, BIGINT, INTEGER, BIGINT).row(1L, 0).row(1L, 1).build().getMaterializedRows());
    }

    @Test
    public void testHashJoin() {
        @Language("SQL") String query = format(
            "" + "SELECT x.orderkey, x.orderstatus " + "FROM (%s) x,(%s) y where x.orderkey=y.orderkey", VALUES,
            VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), resultBuilder(TEST_SESSION, BIGINT, INTEGER).row(1L, 1)
            .row(1L, 1)
            .row(1L, 0)
            .row(1L, 0)
            .row(2L, 2)
            .row(2L, 2)
            .row(3L, 2)
            .row(3L, 2)
            .row(3L, 2)
            .row(3L, 2)
            .row(2L, 3)
            .row(2L, 3)
            .build()
            .getMaterializedRows());
    }

    @Test
    public void testNotSupportType() {
        String VALUES = "" + "SELECT *\n" + "FROM (\n" + "  VALUES\n" + "    ( CAST(true AS boolean), 2),\n"
            + "    ( CAST(false AS boolean), 2)\n" + ") AS orders (orderkey, orderstatus)";

        @Language("SQL") String query = format(
            "SELECT x.orderkey, x.orderstatus " + "FROM (%s) x order by orderkey limit 1", VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(),
            resultBuilder(TEST_SESSION, BIGINT, INTEGER).row(false, 2).build().getMaterializedRows());
    }

    @Test
    public void testNotSupportOperator() {
        String VALUES = "" + "SELECT *\n" + "FROM (\n" + "  VALUES\n" + "    ( CAST(1 AS BIGINT), 2),\n"
            + "    ( CAST(2 AS BIGINT), 2)\n" + ") AS orders (orderkey, orderstatus)";

        @Language("SQL") String query = format(
           "SELECT x.orderkey, x.orderstatus " + "FROM (%s) x cross join (%s) y", VALUES, VALUES);

        MaterializedResult actual = queryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(),
            resultBuilder(TEST_SESSION, BIGINT, INTEGER)
                .row(1L, 2)
                .row(2L, 2)
                .row(1L, 2)
                .row(2L, 2)
                .build().getMaterializedRows());
    }

}
