/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
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
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * The type Omni limit operator test.
 *
 * @since 2021-11-27
 */
public class OmniLimitOperatorTest {
    @Test
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

    @Test
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
}
