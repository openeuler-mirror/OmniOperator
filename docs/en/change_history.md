# Change History<a name="change_history"></a>
<table>
<thead>
  <tr>
    <th style="width: 30%">Date</th>
    <th style="width: 70%">Description</th>
  </tr>
  </thead>
  <tbody>
  <tr>
    <td>2026-03-30</td>
    <td>This issue is the twelfth official release.<br>Released OmniOperator 2.1.0:<br><li>Added InsertIntoHadoopFsRelationCommand to support insertion into HDFS, WriteFile to support ORC write, Window to support array data segmentation, FileSourceScanExec to support array data read, and LocalLimitExec to support array data truncation. </li><li>Added the expressions: datediff, pmod, charTypeWriteSideCheck, least, concat_ws, and get_json_object.</li></td>
  </tr>
  <tr>
    <td>2025-12-30</td>
    <td>This issue is the eleventh official release.<br>Released OmniOperator 2.0.0:<br>Added the adaptation layer Gluten 1.3 for Spark, and added the concat_ws, regexp, regexp_replace, trim, and floor expressions for SparkExtension.</td>
  </tr>
  <tr>
    <td>2025-06-30</td>
    <td>This issue is the tenth official release.<br><li>Added column-based write in Parquet format for Spark 3.3.1. </li> <li>Added support for CentOS 7.9.</li></td>
  </tr>
  <tr>
    <td>2025-03-30</td>
    <td>This issue is the ninth official release.<br><li>Adapted OmniOperator to the Hive engine. </li><li>Added support for Spark 3.4.3 and Spark 3.5.2.</li></td>
  </tr>
  <tr>
    <td>2024-12-30</td>
    <td>This issue is the eighth official release.<br><li>Adapted OmniOperator to the Hive engine. </li><li>Added operator and expression details for OmniOperator.</li></td>
  </tr>
  <tr>
    <td>2024-09-30</td>
    <td>This issue is the seventh official release.<br><li>Added support for the greatest/contains expression, and skips the rollback of the filter operators that contain a scalar subquery expression. </li><li>Optimized the performance of the TableScan, HashJoin, Shuffle, and RollUp operators. </li><li>Optimized OmniOperator feature deployment, enabling automatic deployment in Yarn mode.</li></td>
  </tr>
  <tr>
    <td>2024-06-30</td>
    <td>This issue is the sixth official release.<br><li>Optimized memory for OmniOperator to support big wide table queries. </li><li>Added the Not expression and the AnsiCast expression in the Spark insert scenario. </li><li>Added support for the Hive engine, improving TPC-DS performance by 20%.</li></td>
  </tr>
  <tr>
    <td>2024-03-30</td>
    <td>This issue is the fifth official release.<br><li>Added support for Spark 3.3.1, improving TPC-DS performance by 30%. </li><li>Added the following content: NullType data type, SubqueryBroadcast, Coalesce, and Limit operators; instr, startswith, endswith, and functions; ORC and Parquet data processing in secure clusters; spilling of the Window and HashAggregator operators; NEON instruction optimization for the HashJoin, Sort, and Aggregator operators.</li></td>
  </tr>
  <tr>
    <td>2023-09-30</td>
    <td>This issue is the fourth official release.<br><li>Improved TPC-DS performance by 30% in Spark 3.1.1. </li><li>Added support for the TopNSort, SortMergeJoin, and Sort fusion operators. </li><li>Added the Parquet data format.</li></td>
  </tr>
  <tr>
    <td>2023-06-30</td>
    <td>This issue is the third official release.<br>Implemented TPC-DS datasets for Spark 3.1.1, resulting in a 20% performance improvement.</td>
  </tr>
  <tr>
    <td>2022-12-30</td>
    <td>This issue is the second official release.<br><li>Added support for Spark 3.1.1, enabled successful execution of 10 Big Data Alliance SQL standard test cases. </li><li>Added support for user-defined functions (UDFs).</li></td>
  </tr>
  <tr>
    <td>2022-06-30</td>
    <td>This issue is the first official release.<br>The OmniRuntime OmniOperator feature of Kunpeng BoostKit for Big Data uses a unified infrastructure to support different engines (such as Spark), reducing repeated optimization work, fully exploring common and heterogeneous computing power, and promoting the Kunpeng ecosystem.</td>
  </tr>
  </tbody>
</table>
