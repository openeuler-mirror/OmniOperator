/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatches;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.limit.OmniLimitOperatorFactory;
import nova.hetu.omniruntime.operator.limit.OmniLimitOperatorFactory.FactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni limit operator test.
 *
 * @since 2021-11-27
 */
public class OmniLimitOperatorTest {
    @Test(enabled = false)
    public void testLimitByTwoColum() {
        DataType[] sourceTypes = {IntDataType.INTEGER, DoubleDataType.DOUBLE};
        Object[][] sourceDatas1 = {{0, 1, 2, 0, 1, 2}, {6.6, 5.5, 4.4, 3.3, 2.2, 1.1}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        OmniLimitOperatorFactory limitOperatorFactory = new OmniLimitOperatorFactory(4);
        OmniOperator limitOperator = limitOperatorFactory.createOperator();
        limitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = limitOperator.getOutput();

        Object[][] expectedDatas1 = {{0, 1, 2, 0}, {6.6, 5.5, 4.4, 3.3}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        limitOperator.close();
        limitOperatorFactory.close();
    }

    @Test(enabled = false)
    public void testLimitWithNull() {
        DataType[] sourceTypes = {IntDataType.INTEGER, DoubleDataType.DOUBLE};
        Object[][] sourceDatas1 = {{0, 1, 2, 3, 4, 5}, {6.6, 5.5, 4.4, 3.3, 2.2, 1.1}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);
        Vec[] inVectors = vecBatch1.getVectors();
        inVectors[0].setNull(2);
        inVectors[0].setNull(3);
        inVectors[1].setNull(3);
        inVectors[1].setNull(4);

        OmniLimitOperatorFactory limitOperatorFactory = new OmniLimitOperatorFactory(6);
        OmniOperator limitOperator = limitOperatorFactory.createOperator();
        limitOperator.addInput(vecBatch1);
        Iterator<VecBatch> results = limitOperator.getOutput();

        Object[][] expectedDatas1 = {{0, 1, null, null, 4, 5}, {6.6, 5.5, 4.4, null, null, 1.1}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);

        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        limitOperator.close();
        limitOperatorFactory.close();
    }

    @Test
    public void testFactoryContextEquals() {
        FactoryContext factory1 = new FactoryContext(6, new OperatorConfig());
        FactoryContext factory2 = new FactoryContext(6, new OperatorConfig());
        FactoryContext factory3 = null;

        assertEquals(factory2, factory1);
        assertEquals(factory1, factory1);
        assertNotEquals(factory3, factory1);
    }

    private static void buildLimitExpectData(Object[][] expectedData1, Object[][] expectedData2,
            int maxRowCount) {
        for (int i = 0; i < expectedData1[0].length; i++) {
            expectedData1[0][i] = i;
            expectedData1[1][i] = i + 1;
            expectedData1[2][i] = i + 2L;
            expectedData1[3][i] = "abc" + i;
        }

        for (int i = 0; i < expectedData2[0].length; i++) {
            expectedData2[0][i] = i + maxRowCount;
            expectedData2[1][i] = i + maxRowCount + 1;
            expectedData2[2][i] = i + maxRowCount + 2L;
            expectedData2[3][i] = "abc" + (i + maxRowCount);
        }
    }

    @Test(enabled = false)
    public void testLimitMultiBatchGetOutput() {
        int dataSize = 32800;
        int maxRowCount = 32768; // 1M / (4 + 4 + 8 + 8)
        DataType[] sourceTypes = {IntDataType.INTEGER, IntDataType.INTEGER, LongDataType.LONG, new VarcharDataType(8)};
        Object[][] sourceData1 = new Object[sourceTypes.length][maxRowCount];
        Object[][] sourceData2 = new Object[sourceTypes.length][dataSize - maxRowCount];

        for (int i = 0; i < maxRowCount; i++) {
            sourceData1[0][i] = i;
            sourceData1[1][i] = i + 1;
            sourceData1[2][i] = i + 2L;
            sourceData1[3][i] = "abc" + i;
        }
        for (int i = 0; i < dataSize - maxRowCount; i++) {
            sourceData2[0][i] = i + maxRowCount;
            sourceData2[1][i] = i + maxRowCount + 1;
            sourceData2[2][i] = i + maxRowCount + 2L;
            sourceData2[3][i] = "abc" + (i + maxRowCount);
        }

        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceData1);
        VecBatch vecBatch2 = createVecBatch(sourceTypes, sourceData2);

        int limitSize = 32780;
        OmniLimitOperatorFactory limitOperatorFactory = new OmniLimitOperatorFactory(limitSize);
        OmniOperator limitOperator = limitOperatorFactory.createOperator();
        limitOperator.addInput(vecBatch1);

        List<VecBatch> resultList = new ArrayList<>();
        Iterator<VecBatch> limitIterator = limitOperator.getOutput();
        resultList.add(limitIterator.next());
        limitOperator.addInput(vecBatch2);
        while (limitIterator.hasNext()) {
            resultList.add(limitIterator.next());
        }
        assertEquals(resultList.size(), 2);

        Object[][] expectedData1 = new Object[sourceTypes.length][maxRowCount];
        Object[][] expectedData2 = new Object[sourceTypes.length][limitSize - maxRowCount];

        buildLimitExpectData(expectedData1, expectedData2, maxRowCount);
        assertVecBatchEquals(resultList.get(0), expectedData1);
        assertVecBatchEquals(resultList.get(1), expectedData2);

        freeVecBatches(resultList);
        limitOperator.close();
        limitOperatorFactory.close();
    }
}
