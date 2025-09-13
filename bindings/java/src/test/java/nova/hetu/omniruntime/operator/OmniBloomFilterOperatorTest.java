/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.filter.OmniBloomFilterOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * The type Omni bloom filter operator test.
 *
 * @since 2023-03-03
 */
public class OmniBloomFilterOperatorTest {
    @Test
    public void testCreateBloomFilter() {
        DataType[] types = {IntDataType.INTEGER};
        Object[][] datas = {{1, 6, 4, 0, 0, 0, 0, 0, 0, 0, 0}};
        VecBatch inputVecBatch = createVecBatch(types, datas);

        OmniBloomFilterOperatorFactory factory = new OmniBloomFilterOperatorFactory(1);
        OmniOperator op = factory.createOperator();
        op.addInput(inputVecBatch);
        Iterator<VecBatch> results = op.getOutput();
        VecBatch resultVecBatch = results.next();
        assertEquals(resultVecBatch.getVectorCount(), 1);
        assertEquals(resultVecBatch.getRowCount(), 1);
        freeVecBatch(resultVecBatch);
        op.close();
        factory.close();
    }
}