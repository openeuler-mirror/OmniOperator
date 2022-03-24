/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;

import nova.hetu.omniruntime.operator.limit.OmniDistinctLimitOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

public class OmniDistinctLimitOperatorTest {
    @Test
    public void testDistinctLimitBasic() {
        DataType[] sourceTypes = {IntDataType.INTEGER, DoubleDataType.DOUBLE, VarcharDataType.VARCHAR};
        Object[][] sourceDatas1 = {{0, 1, 2, 0, 1, 2}, {6.6, 5.5, 4.4, 6.6, 5.5, 1.1},
                {"abc", "hello", "world", "abc", "helle", "test"}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        int distinctCols[] = {0, 1, 2};
        OmniDistinctLimitOperatorFactory distinctLimitOperatorFactory = new OmniDistinctLimitOperatorFactory(
                sourceTypes, distinctCols, -1, sourceDatas1[0].length - 1);
        OmniOperator distinctLimitOperator = distinctLimitOperatorFactory.createOperator();
        distinctLimitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = distinctLimitOperator.getOutput();

        Object[][] expectedDatas1 = {{0, 1, 2, 1, 2}, {6.6, 5.5, 4.4, 5.5, 1.1},
                {"abc", "hello", "world", "helle", "test"}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        distinctLimitOperator.close();
        distinctLimitOperatorFactory.close();
    }

    @Test
    public void testDistinctLimitColTypesCover() {
        /*
         * 0: hash col 3&4&5: distinct col normal cols are different but hash cols has
         * conflict(repeat data)
         */
        DataType[] sourceTypes = {LongDataType.LONG, IntDataType.INTEGER, VarcharDataType.VARCHAR, IntDataType.INTEGER,
                DoubleDataType.DOUBLE, VarcharDataType.VARCHAR};
        Object[][] sourceDatas1 = {{10000L, 20000L, 10000L}, {3, 4, 5}, {"aaa", "bbb", "ccc"}, {0, 1, 0},
                {6.6, 5.5, 6.6}, {"hello", "world", "hello"}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        int distinctCols[] = {3, 4, 5};
        OmniDistinctLimitOperatorFactory distinctLimitOperatorFactory = new OmniDistinctLimitOperatorFactory(
                sourceTypes, distinctCols, 0, sourceDatas1[0].length);
        OmniOperator distinctLimitOperator = distinctLimitOperatorFactory.createOperator();
        distinctLimitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = distinctLimitOperator.getOutput();

        // out put order: distinct cols => normal cols => hash col
        Object[][] expectedDatas1 = {{0, 1}, {6.6, 5.5}, {"hello", "world"}, {10000L, 20000L}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        distinctLimitOperator.close();
        distinctLimitOperatorFactory.close();
    }

    @Test
    public void testDistinctLimitWithNull() {
        DataType[] sourceTypes = {IntDataType.INTEGER, DoubleDataType.DOUBLE, VarcharDataType.VARCHAR};
        Object[][] sourceDatas1 = {{0, 1, 2, 0, null, 2, null, null, 2, null},
                {6.6, 5.5, 4.4, 6.6, 5.5, null, null, 5.5, null, null},
                {"abc", "hello", "world", null, "hello", "world", null, "hello", "world", null}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        int distinctCols[] = {0, 1, 2};
        OmniDistinctLimitOperatorFactory distinctLimitOperatorFactory = new OmniDistinctLimitOperatorFactory(
                sourceTypes, distinctCols, -1, sourceDatas1[0].length);
        OmniOperator distinctLimitOperator = distinctLimitOperatorFactory.createOperator();
        distinctLimitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = distinctLimitOperator.getOutput();

        Object[][] expectedDatas1 = {{0, 1, 2, 0, null, 2, null}, {6.6, 5.5, 4.4, 6.6, 5.5, null, null},
                {"abc", "hello", "world", null, "hello", "world", null}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        distinctLimitOperator.close();
        distinctLimitOperatorFactory.close();
    }

    @Test
    public void testDistinctLimitWithHashCol() {
        DataType[] sourceTypes = {IntDataType.INTEGER, DoubleDataType.DOUBLE, LongDataType.LONG};
        Object[][] sourceDatas1 = {{0, 1, 2, 0, 1}, {6.6, 5.5, 4.4, 6.6, 2.2},
                {100000L, 110000L, 120000L, 100000L, 110000L}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        int distinctCols[] = {0, 1};
        OmniDistinctLimitOperatorFactory distinctLimitOperatorFactory = new OmniDistinctLimitOperatorFactory(
                sourceTypes, distinctCols, 2, sourceDatas1[0].length);
        OmniOperator distinctLimitOperator = distinctLimitOperatorFactory.createOperator();
        distinctLimitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = distinctLimitOperator.getOutput();

        Object[][] expectedDatas1 = {{0, 1, 2, 1}, {6.6, 5.5, 4.4, 2.2}, {100000L, 110000L, 120000L, 110000L}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        distinctLimitOperator.close();
        distinctLimitOperatorFactory.close();
    }
}
