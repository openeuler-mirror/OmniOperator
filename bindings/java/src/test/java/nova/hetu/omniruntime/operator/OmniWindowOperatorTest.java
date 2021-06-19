package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;
import nova.hetu.omniruntime.operator.window.OmniWindowOperatorFactory;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.vector.VecType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class OmniWindowOperatorTest
{
    @Test
    public void testRank()
    {
        VecType[] sourceTypes = {VecType.LONG, VecType.LONG};
        int[] outputChannels = {0, 1};
        WindowFunctionType[] windowFunction = {WindowFunctionType.WIN_RANK};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {1};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        int expectedPositions = 10000;
        int[] argumentChannels = {};
        VecType[] windowFunctionReturnType = {VecType.LONG};
        OmniWindowOperatorFactory omniWindowOperatorFactory = new OmniWindowOperatorFactory(sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels, windowFunctionReturnType);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        ImmutableList<VecBatch> vecBatches = buildData();

        omniOperator.addInput(vecBatches);
        Iterator<VecBatch> output = omniOperator.getOutput();
        if (output.hasNext()) {
            VecBatch vecBatch = output.next();
            Vec[] vectors = vecBatch.getVectors();
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
        }
    }

    private ImmutableList<VecBatch> buildData()
    {
        ImmutableList.Builder<VecBatch> vecBatchList = ImmutableList.builder();
        List<Vec> columns = new ArrayList<>();
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
        columns.add(longVec1);
        columns.add(longVec2);
        VecBatch vecBatch = new VecBatch(columns, rowNum);
        vecBatchList.add(vecBatch);
        return vecBatchList.build();
    }
}
