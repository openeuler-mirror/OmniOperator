/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.topn.OmniTopNOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni TopN operator test.
 *
 * @since 2021-7-31
 */
public class OmniTopNOperatorTest {
    @Test
    public void testOneColumn() {
        int rowSize = 6;
        int expectedRowSize = 5;
        long[] rawData = {0, 1, 2, 0, 1, 2};
        LongVec longVec = new LongVec(6);
        longVec.put(rawData, 0, 0, rowSize);
        ArrayList<Vec> longVecs = new ArrayList<>();
        longVecs.add(longVec);

        DataType[] sourceTypes = {LongDataType.LONG};
        String[] sortCols = {"#0"};
        int[] sortAsc = {0};
        int[] nullFirst = {0};
        OmniTopNOperatorFactory omniTopNOperatorFactory = new OmniTopNOperatorFactory(sourceTypes, expectedRowSize,
                sortCols, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();
        operator.addInput(new VecBatch(longVecs));
        Iterator<VecBatch> output = operator.getOutput();
        VecBatch result = output.next();
        assertEquals(result.getRowCount(), expectedRowSize);
        Vec vector = result.getVectors()[0];
        long[] resultArray = new long[expectedRowSize];
        for (int i = 0; i < vector.getSize(); i++) {
            resultArray[i] = ((LongVec) vector).get(i);
        }
        long[] expectedArray = {2, 2, 1, 1, 0};
        assertEquals(resultArray, expectedArray);

        TestUtils.freeVecBatch(result);

        operator.close();
        omniTopNOperatorFactory.close();
    }

    @Test
    public void testMultipleColumns() {
        int rowSize = 6;
        int[] rawData1 = {0, 1, 2, 0, 1, 2};
        long[] rawData2 = {0, 1, 2, 3, 4, 5};
        double[] rawData3 = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
        IntVec vec1 = new IntVec(6);
        LongVec vec2 = new LongVec(6);
        DoubleVec vec3 = new DoubleVec(6);
        vec1.put(rawData1, 0, 0, rowSize);
        vec2.put(rawData2, 0, 0, rowSize);
        vec3.put(rawData3, 0, 0, rowSize);
        ArrayList<Vec> longVecs = new ArrayList<>();
        longVecs.add(vec1);
        longVecs.add(vec2);
        longVecs.add(vec3);

        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        String[] sortCols = {"#0", "#1"};
        int[] sortAsc = {1, 1};
        int[] nullFirst = {0, 0};
        int expectedRowSize = 5;
        OmniTopNOperatorFactory omniTopNOperatorFactory = new OmniTopNOperatorFactory(sourceTypes, expectedRowSize,
                sortCols, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();
        operator.addInput(new VecBatch(longVecs));
        Iterator<VecBatch> output = operator.getOutput();
        VecBatch result = output.next();
        assertEquals(result.getRowCount(), expectedRowSize);
        Vec[] vector = result.getVectors();
        int[] resultArray1 = new int[expectedRowSize];
        long[] resultArray2 = new long[expectedRowSize];
        double[] resultArray3 = new double[expectedRowSize];
        for (int i = 0; i < vector[0].getSize(); i++) {
            resultArray1[i] = ((IntVec) vector[0]).get(i);
        }
        for (int i = 0; i < vector[1].getSize(); i++) {
            resultArray2[i] = ((LongVec) vector[1]).get(i);
        }
        for (int i = 0; i < vector[2].getSize(); i++) {
            resultArray3[i] = ((DoubleVec) vector[2]).get(i);
        }
        int[] expectedArray1 = {0, 0, 1, 1, 2};
        long[] expectedArray2 = {0, 3, 1, 4, 2};
        double[] expectedArray3 = {6.6, 3.3, 5.5, 2.2, 4.4};
        assertEquals(resultArray1, expectedArray1);
        assertEquals(resultArray2, expectedArray2);
        assertEquals(resultArray3, expectedArray3);

        TestUtils.freeVecBatch(result);

        operator.close();
        omniTopNOperatorFactory.close();
    }

    @Test
    public void testTopNDescMultiColumnSortColumn1() {
        int rowSize = 6;
        int[] rawData1 = {0, 1, 2, 0, 1, 2};
        long[] rawData2 = {0, 1, 2, 3, 4, 5};
        double[] rawData3 = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
        IntVec vec1 = new IntVec(6);
        LongVec vec2 = new LongVec(6);
        DoubleVec vec3 = new DoubleVec(6);
        vec1.put(rawData1, 0, 0, rowSize);
        vec2.put(rawData2, 0, 0, rowSize);
        vec3.put(rawData3, 0, 0, rowSize);
        ArrayList<Vec> longVecs = new ArrayList<>();
        longVecs.add(vec1);
        longVecs.add(vec2);
        longVecs.add(vec3);

        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        String[] sortCols = {"#1"};
        int[] sortAsc = {0};
        int[] nullFirst = {0};
        int expectedRowSize = 5;
        OmniTopNOperatorFactory omniTopNOperatorFactory = new OmniTopNOperatorFactory(sourceTypes, expectedRowSize,
                sortCols, sortAsc, nullFirst);
        OmniOperator operator = omniTopNOperatorFactory.createOperator();
        operator.addInput(new VecBatch(longVecs));
        Iterator<VecBatch> output = operator.getOutput();
        VecBatch result = output.next();
        assertEquals(result.getRowCount(), expectedRowSize);
        Vec[] vector = result.getVectors();
        List<Integer> resultList1 = new ArrayList<>();
        List<Long> resultList2 = new ArrayList<>();
        List<Double> resultList3 = new ArrayList<>();

        for (int i = 0; i < vector[0].getSize(); i++) {
            resultList1.add(((IntVec) vector[0]).get(i));
        }
        for (int i = 0; i < vector[1].getSize(); i++) {
            resultList2.add(((LongVec) vector[1]).get(i));
        }
        for (int i = 0; i < vector[2].getSize(); i++) {
            resultList3.add(((DoubleVec) vector[2]).get(i));
        }

        ArrayList<Integer> expectList1 = new ArrayList<>(Arrays.asList(2, 1, 0, 2, 1));
        ArrayList<Long> expectList2 = new ArrayList<>(Arrays.asList(5L, 4L, 3L, 2L, 1L));
        ArrayList<Double> expectList3 = new ArrayList<>(Arrays.asList(1.1, 2.2, 3.3, 4.4, 5.5));
        assertEquals(expectList1, resultList1);
        assertEquals(expectList2, resultList2);
        assertEquals(expectList3, resultList3);

        TestUtils.freeVecBatch(result);

        operator.close();
        omniTopNOperatorFactory.close();
    }
}
