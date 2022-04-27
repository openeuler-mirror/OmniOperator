/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory;
import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory.JitContext;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.OptionalInt;

/**
 * The type Omni partition out operators test.
 *
 * @since 2021-6-30
 */
public class OmniPartionOutOperatorTest {
    @Test
    public void testPartionOut() {
        OptionalInt nullChannel = OptionalInt.empty();

        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        DataType[] hashChannelTypes = {VarcharDataType.VARCHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new VarcharDataType(3), new VarcharDataType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);
        DataType[] sourceTypes = {VarcharDataType.VARCHAR};

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, false, nullChannel, partitionChannels, partitionCount, bucketToPartition, false,
                hashChannelTypes, hashChannels);
        OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator();
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();
        VecBatch result = results.next();
        assertEquals(result.getRowCount(), 3);

        Object[][] expectedDatas = {{"abc", "de", "f"}};
        assertVecBatchEquals(result, expectedDatas);
        TestUtils.freeVecBatch(result);
        omniOperator.close();
        omniPartitionedOutPutOperatorFactory.close();
    }

    @Test
    public void testPartionOutCache() {
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        DataType[] hashChannelTypes = {VarcharDataType.VARCHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new VarcharDataType(3), new VarcharDataType(3)};
        Object[][] buildDatas = {{"abc", "de", null}, {"abc", "de", null}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);
        DataType[] sourceTypes = {VarcharDataType.VARCHAR};

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, false, nullChannel, partitionChannels, partitionCount, bucketToPartition, false,
                hashChannelTypes, hashChannels);
        OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator();
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();
        VecBatch result = results.next();
        assertEquals(result.getRowCount(), 3);

        Object[][] expectedDatas = {{"abc", "de", null}};
        assertVecBatchEquals(result, expectedDatas);
        TestUtils.freeVecBatch(result);
        omniOperator.close();
        omniPartitionedOutPutOperatorFactory.close();
    }

    @Test
    public void testPartionOutChar() {
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        DataType[] hashChannelTypes = {CharDataType.CHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new CharDataType(3), new CharDataType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);
        DataType[] sourceTypes = {CharDataType.CHAR};

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, false, nullChannel, partitionChannels, partitionCount, bucketToPartition, false,
                hashChannelTypes, hashChannels);
        OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator();
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();
        VecBatch result = results.next();
        assertEquals(result.getRowCount(), 3);

        Object[][] expectedDatas = {{"abc", "de", "f"}};
        assertVecBatchEquals(result, expectedDatas);
        TestUtils.freeVecBatch(result);
        omniOperator.close();
        omniPartitionedOutPutOperatorFactory.close();
    }

    @Test
    public void testFactoryJitContextEquals() {
        DataType[] sourceTypes = {CharDataType.CHAR};
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        DataType[] hashChannelTypes = {CharDataType.CHAR};
        int[] hashChannels = {0};

        JitContext factory1 = new JitContext(sourceTypes, false, nullChannel, partitionChannels, partitionCount,
                bucketToPartition, false, hashChannelTypes, hashChannels, new OperatorConfig());
        JitContext factory2 = new JitContext(sourceTypes, false, nullChannel, partitionChannels, partitionCount,
                bucketToPartition, false, hashChannelTypes, hashChannels, new OperatorConfig());
        JitContext factory3 = null;
        assertTrue(factory1.equals(factory2));
        assertTrue(factory1.equals(factory1));
        assertFalse(factory1.equals(factory3));
    }
}
