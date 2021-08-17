/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;

import nova.hetu.omniruntime.operator.union.OmniUnionOperatorFactory;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.*;

import org.testng.annotations.Test;

import java.util.Iterator;

public class OmniUnionOperatorTest {

    @Test
    public void testUnionByTwoColum() {
        VecType[] sourceTypes = {IntVecType.INTEGER, DoubleVecType.DOUBLE};
        Object[][] sourceDatas1 = {{5, 3, 2, 6, 1, 4, 7, 8}, {5.0, 3.0, 2.0, 6.0, 1.0, 4.0, 7.0, 8.0}};
        VecBatch vecBatch1 = createVecBatch(sourceTypes, sourceDatas1);

        Object[][] sourceDatas2 = {{15, 13, 12, 16, 11, 14, 17, 18}, {15.0, 13.0, 12.0, 16.0, 11.0, 14.0, 17.0, 18.0}};
        VecBatch vecBatch2 = createVecBatch(sourceTypes, sourceDatas2);

        OmniUnionOperatorFactory unionOperatorFactory = new OmniUnionOperatorFactory(sourceTypes, false);
        OmniOperator unionOperator = unionOperatorFactory.createOperator();
        unionOperator.addInput(vecBatch1);
        unionOperator.addInput(vecBatch2);
        Iterator<VecBatch> results = unionOperator.getOutput();

        Object[][] expectedDatas1 = {{5, 3, 2, 6, 1, 4, 7, 8}, {5.0, 3.0, 2.0, 6.0, 1.0, 4.0, 7.0, 8.0}};
        Object[][] expectedDatas2 = {{15, 13, 12, 16, 11, 14, 17, 18}, {15.0, 13.0, 12.0, 16.0, 11.0, 14.0, 17.0, 18.0}};

        VecBatch resultVecBatch1 = results.next();
        assertVecBatchEquals(resultVecBatch1, expectedDatas1);
        VecBatch resultVecBatch2 = results.next();
        assertVecBatchEquals(resultVecBatch2, expectedDatas2);

        vecBatch1.releaseAllVectors();
        vecBatch1.close();
        vecBatch2.releaseAllVectors();
        vecBatch2.close();
        resultVecBatch1.releaseAllVectors();
        resultVecBatch1.close();
        resultVecBatch2.releaseAllVectors();
        resultVecBatch2.close();
    }
}
