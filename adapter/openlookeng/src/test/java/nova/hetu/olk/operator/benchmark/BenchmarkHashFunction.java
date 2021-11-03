/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.benchmark;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.VariableWidthBlock;
import org.testng.annotations.Test;

import static io.airlift.slice.XxHash64.hash;

public class BenchmarkHashFunction {
    int ROW_SIZE = 1000000;
    int WIDTH = 10;
    int ROUND = 10;

    @Test
    public void testVarcharHashPerf() {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) BlockUtil.buildVarcharBlock(ROW_SIZE, WIDTH, 10);
        Slice[] slice = BlockUtil.getBlockSlices(variableWidthBlock, ROW_SIZE, WIDTH);

        long hashVal = 0;
        double sum = 0;
        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < ROW_SIZE; i++) {
                hashVal = hash(10, slice[i]);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("hashVal: " + hashVal);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

    @Test
    public void testLongHashPerf() {
        long hashVal = 0;
        double sum = 0;
        long start = 10000000;

        for (int j = 0; j < ROUND; j++) {
            long startTime = System.nanoTime();

            for (long i = start; i < ROW_SIZE + start; i++) {
                hashVal = hash(i);
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime)/1000_000;
            System.out.println("Round: " + (j + 1) + " time: " + duration + " ms");
            sum += duration;
        }
        System.out.println("hashVal: " + hashVal);
        System.out.println("avg time: " + sum / ROUND + " ms");
    }

}
