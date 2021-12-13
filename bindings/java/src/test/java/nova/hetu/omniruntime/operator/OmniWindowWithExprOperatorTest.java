package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.constants.WindowFunctionType;
import nova.hetu.omniruntime.operator.window.OmniWindowWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni window operator test.
 */
public class OmniWindowWithExprOperatorTest {
    /**
     * Test max.
     */
    @Test
    public void testMax() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG, DoubleVecType.DOUBLE};
        int[] outputChannels = {0, 1, 2};
        WindowFunctionType[] windowFunction = {WindowFunctionType.WIN_MAX};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {0};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        int expectedPositions = 10000;
        String[] argumentKeys = {"ADD:3(#2, 50:3)"};
        VecType[] windowFunctionReturnType = {DoubleVecType.DOUBLE};
        OmniWindowWithExprOperatorFactory omniWindowOperatorFactory = new OmniWindowWithExprOperatorFactory(sourceTypes,
            outputChannels,
            windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts,
            preSortedChannelPrefix, expectedPositions, argumentKeys, windowFunctionReturnType);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        VecBatch vecBatch = buildData();

        omniOperator.addInput(vecBatch);
        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch outputVecBatch = output.next();
        Object[][] expectedDatas = {
                {0,  0, 1, 1, 2, 2},
                {8L, 8L, 4L, 1L, 5L, 2L},
                {6.6D, 3.3D, 2.2D, 5.5D, 1.1D, 4.4D},
                {56.6D, 53.3D, 52.2D, 55.5D, 51.1D, 54.4D},
                {56.6D, 56.6D, 52.2D, 55.5D, 51.1D, 54.4D}
        };
        assertVecBatchEquals(outputVecBatch, expectedDatas);
        freeVecBatch(outputVecBatch);
        freeVecBatch(vecBatch);
    }

    private VecBatch buildData() {
        List<Vec> columns = new ArrayList<>();
        int rowNum = 6;
        IntVec vec1 = new IntVec(rowNum);
        LongVec vec2 = new LongVec(rowNum);
        DoubleVec vec3 = new DoubleVec(rowNum);
        vec1.set(0, 0);
        vec1.set(1, 1);
        vec1.set(2, 2);
        vec1.set(3, 0);
        vec1.set(4, 1);
        vec1.set(5, 2);
        vec2.set(0, 8);
        vec2.set(1, 1);
        vec2.set(2, 2);
        vec2.set(3, 8);
        vec2.set(4, 4);
        vec2.set(5, 5);
        vec3.set(0, 6.6);
        vec3.set(1, 5.5);
        vec3.set(2, 4.4);
        vec3.set(3, 3.3);
        vec3.set(4, 2.2);
        vec3.set(5, 1.1);
        columns.add(vec1);
        columns.add(vec2);
        columns.add(vec3);
        return new VecBatch(columns);
    }
}
