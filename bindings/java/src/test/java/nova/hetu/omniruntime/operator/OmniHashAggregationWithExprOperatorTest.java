/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory.JitContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni hash aggregation with expression operator test.
 *
 * @since 2021-11-11
 */
public class OmniHashAggregationWithExprOperatorTest {
    /**
     * test hashAggregationWithExpr performance whether with jit or not.
     */
    @Test
    public void testHashAggregationWithExprComparePref() {
        String[] groupByChannel = {"#0", "#1"};
        String[] aggChannels = {"#3"};
        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, LongDataType.LONG, LongDataType.LONG};
        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
        DataType[] aggOutputTypes = {LongDataType.LONG, LongDataType.LONG};

        OmniHashAggregationWithExprOperatorFactory factoryWithJit = new OmniHashAggregationWithExprOperatorFactory(
                groupByChannel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, true, false,
                new OperatorConfig(true));
        OmniOperator omniOperatorWithJit = factoryWithJit.createOperator();

        ImmutableList.Builder<VecBatch> vecBatchList1 = ImmutableList.builder();
        int rowNum = 100000;
        int pageCount = 10;
        for (int i = 0; i < pageCount; i++) {
            vecBatchList1.add(new VecBatch(buildDataForCount(rowNum)));
        }

        long start1 = System.currentTimeMillis();
        for (VecBatch vecBatch : vecBatchList1.build()) {
            omniOperatorWithJit.addInput(vecBatch);
        }

        Iterator<VecBatch> outputWithJit = omniOperatorWithJit.getOutput();
        long end1 = System.currentTimeMillis();
        System.out.println("HashAggregationWithExpr with jit use " + (end1 - start1) + " ms.");

        OmniHashAggregationWithExprOperatorFactory factoryWithoutJit = new OmniHashAggregationWithExprOperatorFactory(
                groupByChannel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes, true, false,
                new OperatorConfig(false));
        OmniOperator omniOperatorWithoutJit = factoryWithoutJit.createOperator();

        ImmutableList.Builder<VecBatch> vecBatchList2 = ImmutableList.builder();
        for (int i = 0; i < pageCount; i++) {
            vecBatchList2.add(new VecBatch(buildDataForCount(rowNum)));
        }

        long start2 = System.currentTimeMillis();
        for (VecBatch vecBatch : vecBatchList2.build()) {
            omniOperatorWithoutJit.addInput(vecBatch);
        }

        Iterator<VecBatch> outputWithoutJit = omniOperatorWithoutJit.getOutput();
        long end2 = System.currentTimeMillis();
        System.out.println("HashAggregationWithExpr without jit use " + (end2 - start2) + " ms.");

        while (outputWithJit.hasNext()) {
            VecBatch resultWithJit = outputWithJit.next();
            VecBatch resultWithoutJit = outputWithoutJit.next();
            assertVecBatchEquals(resultWithJit, resultWithoutJit);
            freeVecBatch(resultWithJit);
            freeVecBatch(resultWithoutJit);
        }

        omniOperatorWithJit.close();
        omniOperatorWithoutJit.close();
        factoryWithJit.close();
        factoryWithoutJit.close();
    }

    @Test
    public void testHashAggWithPartialExpr() {
        String[] groupByChanel = {"MODULUS:2(#0, 3:2)", "#2"};
        String[] aggChannels = {"MULTIPLY:2(#1, 5:2)", "#3"};

        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        DataType[] aggOutputTypes = {LongDataType.LONG, DoubleDataType.DOUBLE};

        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, IntDataType.INTEGER, IntDataType.INTEGER};

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

        freeVecBatch(resultVecBatch);
        omniOperator.close();
        factory.close();
    }

    @Test
    public void testHashAggWithAllExpr() {
        String[] groupByChanel = {"MODULUS:2(#0, 3:2)", "ADD:1(#2, 5:1)"};
        String[] aggChannels = {"MULTIPLY:2(#1, 5:2)", "ADD:1(#3, 5:1)"};

        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        DataType[] aggOutputTypes = {LongDataType.LONG, DoubleDataType.DOUBLE};

        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, IntDataType.INTEGER, IntDataType.INTEGER};

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

        freeVecBatch(resultVecBatch);
        omniOperator.close();
        factory.close();
    }

    @Test
    public void testHashAggWithNoExpr() {
        String[] groupByChanel = {"#0", "#2"};
        String[] aggChannels = {"#1", "#3"};

        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        DataType[] aggOutputTypes = {LongDataType.LONG, DoubleDataType.DOUBLE};

        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, IntDataType.INTEGER, IntDataType.INTEGER};

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

        freeVecBatch(resultVecBatch);
        omniOperator.close();
        factory.close();
    }

    @Test
    public void testFactoryJitContextEquals() {
        String[] groupByChanel = {"#0", "#2"};
        String[] aggChannels = {"#1", "#3"};

        FunctionType[] aggFunctionTypes = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
        DataType[] aggOutputTypes = {LongDataType.LONG, DoubleDataType.DOUBLE};

        DataType[] sourceTypes = {LongDataType.LONG, LongDataType.LONG, IntDataType.INTEGER, IntDataType.INTEGER};

        JitContext factory1 = new JitContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes,
                true, false, new OperatorConfig());
        JitContext factory2 = new JitContext(groupByChanel, aggChannels, sourceTypes, aggFunctionTypes, aggOutputTypes,
                true, false, new OperatorConfig());
        JitContext factory3 = null;

        assertTrue(factory1.equals(factory2));
        assertTrue(factory1.equals(factory1));
        assertFalse(factory1.equals(factory3));
    }

    private List<Vec> buildDataForCount(int rowNum) {
        LongVec c1 = new LongVec(rowNum);
        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 1);
            c2.set(i, 1);
        }

        LongVec c3 = new LongVec(rowNum);
        LongVec c4 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            if (i % 2 == 0) {
                c3.set(i, 1);
                c4.set(i, 1);
            } else {
                c3.setNull(i);
                c4.setNull(i);
            }
        }

        List<Vec> columns = new ArrayList<>();
        columns.add(c1);
        columns.add(c2);
        columns.add(c3);
        columns.add(c4);

        return columns;
    }
}
