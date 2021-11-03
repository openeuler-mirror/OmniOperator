/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.benchmark;

import io.prestosql.spi.block.VariableWidthBlock;
import org.testng.annotations.Test;

public class BenchmarkEqualsFunction {
    int ROW_SIZE = 1000000;
    int WIDTH = 10;
    int ROUND = 10;

    @Test
    public void testSameBlockEqualsPerf() {
        VariableWidthBlock variableWidthBlock1 = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 10);
        VariableWidthBlock variableWidthBlock2 = (VariableWidthBlock) variableWidthBlock1.copyRegion(0, variableWidthBlock1.getPositionCount());

        boolean isEqual = false;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                isEqual = variableWidthBlock1.equals(i, 0, variableWidthBlock2, i, 0, WIDTH);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("isEqual: " + isEqual);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

    @Test
    public void testDiffBlockEqualsPerf() {
        VariableWidthBlock variableWidthBlock1 = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 10);
        VariableWidthBlock variableWidthBlock2 = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 20);

        boolean isEqual = true;
        double sum = 0;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                isEqual = variableWidthBlock1.equals(i, 0, variableWidthBlock2, i, 0, WIDTH);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("isEqual: " + isEqual);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

}
