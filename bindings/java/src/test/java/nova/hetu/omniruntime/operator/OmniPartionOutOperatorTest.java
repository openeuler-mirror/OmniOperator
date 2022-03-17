
package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.type.DataType;
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
        DataType[] sourceTypes = {VarcharDataType.VARCHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();

        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        DataType[] hashChannelTypes = {VarcharDataType.VARCHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new VarcharDataType(3), new VarcharDataType(3)};
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
        DataType[] sourceTypes = {VarcharDataType.VARCHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        DataType[] hashChannelTypes = {VarcharDataType.VARCHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new VarcharDataType(3), new VarcharDataType(3)};
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
        DataType[] sourceTypes = {CharDataType.CHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel = OptionalInt.empty();
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        DataType[] hashChannelTypes = {CharDataType.CHAR};
        int[] hashChannels = {0};

        DataType[] buildTypes = {new CharDataType(3), new CharDataType(3)};
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
