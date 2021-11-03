/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.benchmark;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.VariableWidthBlock;
import org.testng.annotations.Test;

public class BenchmarkCompareFunction {
    int ROW_SIZE = 1000000;
    int WIDTH = 10;
    int ROUND = 10;

    VariableWidthBlock variableWidthBlock1 = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 10);
    VariableWidthBlock variableWidthBlock2 = (VariableWidthBlock) variableWidthBlock1.copyRegion(0, variableWidthBlock1.getPositionCount());
    VariableWidthBlock variableWidthBlock3 = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 20);
    Slice[] slice1 = BlockUtil.getBlockSlices(variableWidthBlock1, ROW_SIZE, WIDTH);
    Slice[] slice2 = BlockUtil.getBlockSlices(variableWidthBlock2, ROW_SIZE, WIDTH);
    Slice[] slice3 = BlockUtil.getBlockSlices(variableWidthBlock3, ROW_SIZE, WIDTH);

    @Test
    public void testSameSliceComparePerf() {
        int comp = 0;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                comp = slice1[i].compareTo(slice2[i]);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("comp: " + comp);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

    @Test
    public void testDiffSliceComparePerf() {
        int comp = 0;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                comp = slice1[i].compareTo(slice3[i]);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("comp: " + comp);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

    @Test
    public void testSameBlockComparePerf() {
        int comp = 0;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                comp = variableWidthBlock1.compareTo(i, 0, WIDTH, variableWidthBlock2, i, 0, WIDTH);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("comp: " + comp);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

    @Test
    public void testDiffBlockComparePerf() {
        int comp = 0;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                comp = variableWidthBlock1.compareTo(i, 0, WIDTH, variableWidthBlock3, i, 0, WIDTH);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("comp: " + comp);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

}
