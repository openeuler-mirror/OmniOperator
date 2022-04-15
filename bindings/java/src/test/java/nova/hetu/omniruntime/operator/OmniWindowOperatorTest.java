/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.window.OmniWindowOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni window operator test.
 *
 * @since 2021-6-4
 */
public class OmniWindowOperatorTest {
    /**
     * Test rank.
     */
    @Test
    public void testRank() {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] outputChannels = {0, 1};
        FunctionType[] windowFunction = {FunctionType.OMNI_WINDOW_TYPE_RANK};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {1};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        int expectedPositions = 10000;
        int[] argumentChannels = {};
        DataType[] windowFunctionReturnType = {LongDataType.LONG};
        OmniWindowOperatorFactory omniWindowOperatorFactory = new OmniWindowOperatorFactory(sourceTypes, outputChannels,
                windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts,
                preSortedChannelPrefix, expectedPositions, argumentChannels, windowFunctionReturnType);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        VecBatch vecBatch = buildData();

        omniOperator.addInput(vecBatch);
        Iterator<VecBatch> output = omniOperator.getOutput();
        if (output.hasNext()) {
            VecBatch outputVecBatch = output.next();
            Vec[] vectors = outputVecBatch.getVectors();
            assertEquals(((LongVec) vectors[0]).get(0), 1);
            assertEquals(((LongVec) vectors[0]).get(1), 1);
            assertEquals(((LongVec) vectors[0]).get(2), 1);
            assertEquals(((LongVec) vectors[0]).get(3), 2);
            assertEquals(((LongVec) vectors[0]).get(4), 2);
            assertEquals(((LongVec) vectors[1]).get(0), 2);
            assertEquals(((LongVec) vectors[1]).get(1), 4);
            assertEquals(((LongVec) vectors[1]).get(2), 6);
            assertEquals(((LongVec) vectors[1]).get(3), -1);
            assertEquals(((LongVec) vectors[1]).get(4), 5);
            assertEquals(((LongVec) vectors[2]).get(0), 1);
            assertEquals(((LongVec) vectors[2]).get(1), 2);
            assertEquals(((LongVec) vectors[2]).get(2), 3);
            assertEquals(((LongVec) vectors[2]).get(3), 1);
            assertEquals(((LongVec) vectors[2]).get(4), 2);
            freeVecBatch(outputVecBatch);
        }

        omniOperator.close();
        omniWindowOperatorFactory.close();
    }

    /**
     * Test count.
     */
    @Test
    public void testCount() {
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG};
        int[] outputChannels = {0, 1};
        FunctionType[] windowFunction = {FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {1};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        int expectedPositions = 10000;
        int[] argumentChannels = {1, -1};
        DataType[] windowFunctionReturnType = {LongDataType.LONG, LongDataType.LONG};
        OmniWindowOperatorFactory omniWindowOperatorFactory = new OmniWindowOperatorFactory(sourceTypes, outputChannels,
                windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts,
                preSortedChannelPrefix, expectedPositions, argumentChannels, windowFunctionReturnType);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        VecBatch vecBatch = buildData();
        vecBatch.getVectors()[1].setNull(2);

        omniOperator.addInput(vecBatch);
        Iterator<VecBatch> output = omniOperator.getOutput();
        if (output.hasNext()) {
            VecBatch outputVecBatch = output.next();
            Object[][] expectedData = {{1L, 1L, 1L, 2L, 2L}, {2L, 6L, null, -1L, 5L}, {1L, 2L, 2L, 1L, 2L},
                    {1L, 2L, 3L, 1L, 2L}};

            assertEquals(outputVecBatch.getRowCount(), 5);
            assertEquals(outputVecBatch.getVectors().length, 4);
            assertVecBatchEquals(outputVecBatch, expectedData);

            freeVecBatch(outputVecBatch);
        }

        omniOperator.close();
        omniWindowOperatorFactory.close();
    }

    private VecBatch buildData() {
        int rowNum = 5;
        LongVec longVec1 = new LongVec(rowNum);
        LongVec longVec2 = new LongVec(rowNum);
        longVec1.set(0, 2);
        longVec1.set(1, 1);
        longVec1.set(2, 1);
        longVec1.set(3, 2);
        longVec1.set(4, 1);
        longVec2.set(0, -1);
        longVec2.set(1, 2);
        longVec2.set(2, 4);
        longVec2.set(3, 5);
        longVec2.set(4, 6);

        List<Vec> columns = new ArrayList<>();
        columns.add(longVec1);
        columns.add(longVec2);
        return new VecBatch(columns, rowNum);
    }
}
