package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * The type Omni hash aggregation with expression operator test.
 */
public class OmniHashAggregationWithExprOperatorTest {

    @Test
    public void testHashAggWithPartialExpr() {
        String[] groupByChanel = {"MODULUS:long(#0, 3)", "#2"};
        String[] aggChannels = {"MULTIPLY:long(#1, 5)", "#3"};

        AggType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        VecType[] aggOutputTypes = {LongVecType.LONG, DoubleVecType.DOUBLE};

        VecType[] sourceTypes = {LongVecType.LONG, LongVecType.LONG, IntVecType.INTEGER, IntVecType.INTEGER};

        OmniHashAggregationWithExprOperatorFactory factory = new OmniHashAggregationWithExprOperatorFactory(
            groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, true, false);

        OmniOperator omniOperator = factory.createOperator();

        Object[][] sourceDatas = {{2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L},
            {5, 5, 5, 5, 5, 5, 5, 5}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();

        assertEquals(results.hasNext(), true);
        VecBatch resultVecBatch = results.next();
        assertEquals(results.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), 1);
        assertEquals(resultVecBatch.getVectorCount(), 4);

        Object[][] expectedDatas = {{2L}, {5}, {180L}, {4.5}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
    }

    @Test
    public void testHashAggWithAllExpr() {
        String[] groupByChanel = {"MODULUS:long(#0, 3)", "ADD:int(#2, 5)"};
        String[] aggChannels = {"MULTIPLY:long(#1, 5)", "ADD:int(#3, 5)"};

        AggType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        VecType[] aggOutputTypes = {LongVecType.LONG, DoubleVecType.DOUBLE};

        VecType[] sourceTypes = {LongVecType.LONG, LongVecType.LONG, IntVecType.INTEGER, IntVecType.INTEGER};

        OmniHashAggregationWithExprOperatorFactory factory = new OmniHashAggregationWithExprOperatorFactory(
            groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, true, false);

        OmniOperator omniOperator = factory.createOperator();

        Object[][] sourceDatas = {{2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L},
            {5, 5, 5, 5, 5, 5, 5, 5}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();

        assertEquals(results.hasNext(), true);
        VecBatch resultVecBatch = results.next();
        assertEquals(results.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), 1);
        assertEquals(resultVecBatch.getVectorCount(), 4);

        Object[][] expectedDatas = {{2L}, {10}, {180L}, {9.5}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
    }

    @Test
    public void testHashAggWithNoExpr() {
        String[] groupByChanel = {"#0", "#2"};
        String[] aggChannels = {"#1", "#3"};

        AggType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        VecType[] aggOutputTypes = {LongVecType.LONG, DoubleVecType.DOUBLE};

        VecType[] sourceTypes = {LongVecType.LONG, LongVecType.LONG, IntVecType.INTEGER, IntVecType.INTEGER};

        OmniHashAggregationWithExprOperatorFactory factory = new OmniHashAggregationWithExprOperatorFactory(
                groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, true, false);

        OmniOperator omniOperator = factory.createOperator();

        Object[][] sourceDatas = {{2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L}, {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L},
                {5, 5, 5, 5, 5, 5, 5, 5}, {5, 3, 2, 6, 1, 4, 7, 8}};
        VecBatch vecBatch = createVecBatch(sourceTypes, sourceDatas);
        omniOperator.addInput(vecBatch);

        Iterator<VecBatch> results = omniOperator.getOutput();

        assertEquals(results.hasNext(), true);
        VecBatch resultVecBatch = results.next();
        assertEquals(results.hasNext(), false);
        assertEquals(resultVecBatch.getRowCount(), 1);
        assertEquals(resultVecBatch.getVectorCount(), 4);

        Object[][] expectedDatas = {{2L}, {5}, {36L}, {4.5}};
        assertVecBatchEquals(resultVecBatch, expectedDatas);

        freeVecBatch(vecBatch);
        freeVecBatch(resultVecBatch);
    }
}
