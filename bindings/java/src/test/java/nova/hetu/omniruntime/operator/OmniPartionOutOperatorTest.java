package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory;
import nova.hetu.omniruntime.type.CharVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.testng.annotations.Test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;

import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static org.testng.Assert.assertEquals;

public class OmniPartionOutOperatorTest
{
    @Test
    public void testPartionOut(){
        VecType[] sourceTypes = {VarcharVecType.VARCHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel =  OptionalInt.empty();;
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        VecType[] hashChannelTypes = {VarcharVecType.VARCHAR};
        int[] hashChannels = {0};

        VecType[] buildTypes = {new VarcharVecType(3), new VarcharVecType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(sourceTypes,
                replicatesAnyRow, nullChannel, partitionChannels, partitionCount, bucketToPartition, isHashPrecomputed, hashChannelTypes, hashChannels);
        OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator();
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();
        VecBatch result = results.next();
        assertEquals(result.getRowCount(), 3);

        Vec[] vector = result.getVectors();
        List resultList1 = new ArrayList<String>();
        for (int i = 0; i < vector[0].getSize(); i++) {
            resultList1.add(new String(((VarcharVec) vector[0]).get(i)));
        }

        ArrayList<String> expectList1 = new ArrayList<>(Arrays.asList("abc", "de", "f"));
        assertEquals(expectList1,resultList1);
        vecBatch.releaseAllVectors();
        result.releaseAllVectors();
    }

    @Test
    public void testPartionOutChar(){
        VecType[] sourceTypes = {CharVecType.CHAR};
        boolean replicatesAnyRow = false;
        OptionalInt nullChannel =  OptionalInt.empty();;
        int[] partitionChannels = {0};
        int partitionCount = 1;
        int[] bucketToPartition = {0};
        boolean isHashPrecomputed = false;
        VecType[] hashChannelTypes = {CharVecType.CHAR};
        int[] hashChannels = {0};

        VecType[] buildTypes = {new CharVecType(3), new CharVecType(3)};
        Object[][] buildDatas = {{"abc", "de", "f"}, {"def", "bc", "a"}};
        VecBatch vecBatch = createVecBatch(buildTypes, buildDatas);

        OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(sourceTypes,
                replicatesAnyRow, nullChannel, partitionChannels, partitionCount, bucketToPartition, isHashPrecomputed, hashChannelTypes, hashChannels);
        OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator();
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();
        VecBatch result = results.next();
        assertEquals(result.getRowCount(), 3);

        Vec[] vector = result.getVectors();
        List resultList1 = new ArrayList<String>();
        for (int i = 0; i < vector[0].getSize(); i++) {
            resultList1.add(new String(((VarcharVec) vector[0]).get(i)));
        }

        ArrayList<String> expectList1 = new ArrayList<>(Arrays.asList("abc", "de", "f"));
        assertEquals(expectList1,resultList1);
        vecBatch.releaseAllVectors();
        result.releaseAllVectors();
    }
}
