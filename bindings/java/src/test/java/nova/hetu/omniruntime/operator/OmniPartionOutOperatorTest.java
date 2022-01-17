
package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory;
import nova.hetu.omniruntime.type.CharVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.OptionalInt;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static org.testng.Assert.assertEquals;

public class OmniPartionOutOperatorTest {
    @Test
    public void testPartionOut() {
        VecType[] sourceTypes = {VarcharVecType.VARCHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();

        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        VecType[] hashChannelTypes = {VarcharVecType.VARCHAR};
        int[] hashChannels = {0};

        VecType[] buildTypes = {new VarcharVecType(3), new VarcharVecType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, replicatesAnyRow, nullChannel, partitionChannels, partitionCount, bucketToPartition,
                isHashPrecomputed, hashChannelTypes, hashChannels);
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
        VecType[] sourceTypes = {VarcharVecType.VARCHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        VecType[] hashChannelTypes = {VarcharVecType.VARCHAR};
        int[] hashChannels = {0};

        VecType[] buildTypes = {new VarcharVecType(3), new VarcharVecType(3)};
        Object[][] buildDatas = {{"abc", "de", null}, {"abc", "de", null}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, replicatesAnyRow, nullChannel, partitionChannels, partitionCount, bucketToPartition,
                isHashPrecomputed, hashChannelTypes, hashChannels);
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
        VecType[] sourceTypes = {CharVecType.CHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        VecType[] hashChannelTypes = {CharVecType.CHAR};
        int[] hashChannels = {0};

        VecType[] buildTypes = {new CharVecType(3), new CharVecType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                sourceTypes, replicatesAnyRow, nullChannel, partitionChannels, partitionCount, bucketToPartition,
                isHashPrecomputed, hashChannelTypes, hashChannels);
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
}
