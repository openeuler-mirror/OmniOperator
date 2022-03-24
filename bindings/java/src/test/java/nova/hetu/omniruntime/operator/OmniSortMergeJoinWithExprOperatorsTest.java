/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createBlankVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.join.OmniSmjBufferedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniSmjStreamedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

/**
 * The type Omni hash join operators test.
 */
public class OmniSortMergeJoinWithExprOperatorsTest {
    /**
     * Test inner hash join one column 1.
     */
    @Test
    public void testSmjOneTimeEqualCondition() {
        int intputResult = 0;
        DataType[] streamedTypes = {IntDataType.INTEGER, LongDataType.LONG};
        Object[][] streamedDatas1 = {{0, 1, 2, 3, 4, 5}, {6600L, 5500L, 4400L, 3300L, 2200L, 1100L}};
        VecBatch streamedVecBatch1 = createVecBatch(streamedTypes, streamedDatas1);

        String[] streamedKeyExps = {"$operator$ADD:1(#0, 5:1)"};
        int[] streamedOutputCols = {1};
        OmniSmjStreamedTableWithExprOperatorFactory streamedBuilderWithExprOperatorFactory = new OmniSmjStreamedTableWithExprOperatorFactory(
                streamedTypes, streamedKeyExps, streamedOutputCols, OMNI_JOIN_TYPE_INNER, Optional.empty());
        OmniOperator streamedTableOperator = streamedBuilderWithExprOperatorFactory.createOperator();
        System.out.println("create streamed table op");

        DataType[] bufferedTypes = {LongDataType.LONG, IntDataType.INTEGER};
        Object[][] bufferedDatas1 = {{6006L, 5005L, 4004L, 3003L, 2002L, 1001L}, {0, 1, 2, 3, 4, 5}};
        VecBatch bufferedVecBatch1 = createVecBatch(bufferedTypes, bufferedDatas1);

        int[] bufferedOutputCols = {0};
        String[] bufferedKeyExps = {"$operator$ADD:1(#1, 5:1)"};
        OmniSmjBufferedTableWithExprOperatorFactory bufferedWithExprOperatorFactory = new OmniSmjBufferedTableWithExprOperatorFactory(
                bufferedTypes, bufferedKeyExps, bufferedOutputCols, streamedBuilderWithExprOperatorFactory);
        OmniOperator bufferedTableOperator = bufferedWithExprOperatorFactory.createOperator();
        System.out.println("create buffered table op");

        // start to add input
        intputResult = streamedTableOperator.addInput(streamedVecBatch1);
        assertEquals(intputResult, 3);
        System.out.println("add streamed table intput");

        intputResult = bufferedTableOperator.addInput(bufferedVecBatch1);
        assertEquals(intputResult, 3);
        System.out.println("add buffered table intput");

        VecBatch bufferedVecBatchEof = createBlankVecBatch(bufferedTypes);
        intputResult = bufferedTableOperator.addInput(bufferedVecBatchEof);
        assertEquals(intputResult, 2);
        System.out.println("add buffered table intput eof");

        VecBatch streamedVecBatchEof = createBlankVecBatch(streamedTypes);
        intputResult = streamedTableOperator.addInput(streamedVecBatchEof);
        assertEquals(intputResult, 5);
        System.out.println("add streamed table intput eof");

        Iterator<VecBatch> results = bufferedTableOperator.getOutput();
        VecBatch resultVecBatch = results.next();
        System.out.println("fetch table output");

        int len = resultVecBatch.getRowCount();
        assertEquals(len, 6);
        Object[][] expectedDatas = {{6600L, 5500L, 4400L, 3300L, 2200L, 1100L},
                {6006L, 5005L, 4004L, 3003L, 2002L, 1001L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(resultVecBatch);
        bufferedTableOperator.close();
        bufferedWithExprOperatorFactory.close();
        streamedTableOperator.close();
        streamedBuilderWithExprOperatorFactory.close();
    }
}
