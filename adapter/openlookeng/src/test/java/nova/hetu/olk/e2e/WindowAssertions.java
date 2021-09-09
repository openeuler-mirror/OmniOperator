/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.e2e;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;

import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;

import org.intellij.lang.annotations.Language;

public final class WindowAssertions
{
    //since we don't support varchar now, we will use 0 as "O", 1 as "F"
    private static final String VALUES = "" +
            "SELECT *\n" +
            "FROM (\n" +
            "  VALUES\n" +
            "    ( 1, 0),\n" +
            "    ( 2, 0),\n" +
            "    ( 3, 1),\n" +
            "    ( 4, 0),\n" +
            "    ( 5, 1),\n" +
            "    ( 6, 1),\n" +
            "    ( 7, 0),\n" +
            "    (32, 0),\n" +
            "    (33, 1),\n" +
            "    (34, 0)\n" +
            ") AS orders (orderkey, orderstatus)";
    private static final String VALUES1 = "" +
            "SELECT *\n" +
            "FROM (\n" +
            "  VALUES\n" +
            "    ( 1, 0),\n" +
            "    ( 1, 0),\n" +
            "    ( 1, 0),\n" +
            "    ( 2, 0),\n" +
            "    ( 2, 0),\n" +
            "    ( 3, 1),\n" +
            "    ( 3, 1),\n" +
            "    ( 4, 0),\n" +
            "    ( 5, 1),\n" +
            "    ( 6, 1),\n" +
            "    ( 7, 0),\n" +
            "    (32, 0),\n" +
            "    (33, 1),\n" +
            "    (33, 1),\n" +
            "    (34, 0)\n" +
            ") AS orders (orderkey, orderstatus)";

    private static final String VALUES_WITH_NULLS = "" +
            "SELECT *\n" +
            "FROM (\n" +
            "  VALUES\n" +
            "    ( 1,                   CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),\n" +
            "    ( 3,                   'F',                   '1993-10-14'),\n" +
            "    ( 5,                   'F',                   CAST(NULL AS VARCHAR)),\n" +
            "    ( 7,                   CAST(NULL AS VARCHAR), '1996-01-10'),\n" +
            "    (34,                   'O',                   '1998-07-21'),\n" +
            "    ( 6,                   'F',                   '1992-02-21'),\n" +
            "    (CAST(NULL AS BIGINT), 'F',                   '1993-10-27'),\n" +
            "    (CAST(NULL AS BIGINT), 'O',                   '1996-12-01'),\n" +
            "    (CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),\n" +
            "    (CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR), '1995-07-16')\n" +
            ") AS orders (orderkey, orderstatus, orderdate)";

    private WindowAssertions() {}

    public static void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected, LocalQueryRunner localQueryRunner)
    {
        @Language("SQL") String query = format("" +
                "SELECT orderkey, orderstatus,\n%s\n" +
                "FROM (%s) x group by orderstatus,orderkey order by orderkey,orderstatus", sql, VALUES);

        MaterializedResult actual = localQueryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static void assertWindowQuery1(@Language("SQL") String sql, MaterializedResult expected, LocalQueryRunner localQueryRunner)
    {
        @Language("SQL") String query = format("" +
                "SELECT orderkey, orderstatus,\n%s\n" +
                "FROM (%s) x group by orderstatus,orderkey order by orderstatus,orderkey", sql, VALUES1);

        MaterializedResult actual = localQueryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static void assertWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected, LocalQueryRunner localQueryRunner)
    {
        MaterializedResult actual = executeWindowQueryWithNulls(sql, localQueryRunner);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static MaterializedResult executeWindowQueryWithNulls(@Language("SQL") String sql, LocalQueryRunner localQueryRunner)
    {
        @Language("SQL") String query = format("" +
                "SELECT orderkey, orderstatus,\n%s\n" +
                "FROM (%s) x", sql, VALUES_WITH_NULLS);

        return localQueryRunner.execute(query);
    }
}
