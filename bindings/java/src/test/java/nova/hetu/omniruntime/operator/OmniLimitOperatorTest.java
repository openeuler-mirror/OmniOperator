/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.limit.OmniLimitOperatorFactory;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.annotations.Test;

import java.util.Iterator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;

public class OmniLimitOperatorTest {
    @Test
    public void testLimitByTwoColum() {
        VecType[] sourceTypes = {IntVecType.INTEGER, DoubleVecType.DOUBLE};
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
        VecType[] sourceTypes = {IntVecType.INTEGER, DoubleVecType.DOUBLE};
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
}
