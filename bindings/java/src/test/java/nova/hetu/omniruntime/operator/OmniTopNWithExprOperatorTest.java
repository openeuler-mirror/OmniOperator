package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.topn.OmniTopNWithExprOperatorFactory;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

public class OmniTopNWithExprOperatorTest {

    @Test
    public void testTopNWithAllExpr() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG, LongVecType.LONG};
        String[] sortKeys = {"ADD:1(#0, 5)", "MODULUS:2(#2, 3)"};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
            expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
            {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), sourceTypes.length + sortKeys.length);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 5L, 4L, 3L}, {8L, 7L, 3L, 1L, 2L},
            {20, 18, 13, 13, 13}, {2L, 1L, 0L, 1L, 2L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
        operator.close();
    }

    @Test
    public void testTopNWithPartialExpr() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG, LongVecType.LONG};
        String[] sortKeys = {"#0", "MODULUS:2(#2, 3)"};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
                expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
                {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), 4);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 5L, 4L, 3L}, {8L, 7L, 3L, 1L, 2L},
            {2L, 1L, 0L, 1L, 2L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
        operator.close();
    }

    @Test
    public void testTopNWithNoExpr() {
        VecType[] sourceTypes = {IntVecType.INTEGER, LongVecType.LONG, LongVecType.LONG};
        String[] sortKeys = {"#0", "#2"};
        int[] sortAsc = {0, 1};
        int[] nullFirst = {0, 0};

        int expectedRowSize = 5;

        OmniTopNWithExprOperatorFactory omniTopNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes,
            expectedRowSize, sortKeys, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();

        Object[][] sourceDatas = {{5, 8, 8, 6, 8, 4, 13, 15}, {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L},
            {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);

        operator.addInput(vecBatch);
        Iterator<VecBatch> output = operator.getOutput();

        assertEquals(output.hasNext(), true);
        VecBatch resultVecBatch = output.next();
        assertEquals(output.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), expectedRowSize);
        assertEquals(resultVecBatch.getVectorCount(), 3);

        Object[][] expectedDatas = {{15, 13, 8, 8, 8}, {23L, 0L, 4L, 3L, 5L}, {8L, 7L, 1L, 2L, 3L}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
        operator.close();
    }
}
