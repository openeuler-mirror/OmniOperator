package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.vector.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import java.nio.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OmniFilterAndProjectOperatorTest {

    private ImmutableList<VecBatch> makeInput(int nRows, Vec ...cols) {
        return ImmutableList.copyOf(new VecBatch[] {new VecBatch(cols, nRows)});
    }

    @Test
    public void doubles()
    {
        VecType[] types = {VecType.DOUBLE};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$LESS_THAN(#0, 1.0)",
            types,
            projectIndices
        );
        final int NUM_ROWS = 5000;
        DoubleVec col1 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 2 == 0 ? 0.5 : 1.5);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 2500);
        DoubleBuffer res1 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        while (res1.hasRemaining()) {
            Assert.assertTrue(res1.get() < 1);
        }
    }

    @Test
    public void lessThan()
    {
        VecType[] types = {VecType.INT};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN(#0, 2000)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 5000;
        IntVec col1 = new IntVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 2000);
        IntBuffer res1 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res1.hasRemaining()) {
            Assert.assertTrue(res1.get() < 2000);
        }
    }

    @Test
    public void greaterThan()
    {
        VecType[] types = {VecType.INT, VecType.LONG};
        int[] projectIndices = {0, 1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN(#0, 20)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 5000;
        IntVec col1 = new IntVec(NUM_ROWS);
        LongVec col2 = new LongVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 25);
            col2.set(i, 3000000000L);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1, col2));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 800);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        LongBuffer res1 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        while (res0.hasRemaining()) {
            Assert.assertTrue(res0.get() > 20);
            Assert.assertEquals(res1.get(), 3000000000L);
        }
    }

    @Test(enabled = false)
    public void equalTo()
    {
        VecType[] types = {VecType.INT, VecType.LONG, VecType.DOUBLE};
        int[] projectIndices = {2, 1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$EQUAL(#2, 50)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 5000;
        IntVec col1 = new IntVec(NUM_ROWS);
        LongVec col2 = new LongVec(NUM_ROWS);
        DoubleVec col3 =  new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col2.set(i, i % 100);
            col3.set(i, i % 100);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1, col2));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 50);
        DoubleBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        LongBuffer res1 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        while (res0.hasRemaining()) {
            Assert.assertEquals(res0.get(), 50);
            Assert.assertEquals(res1.get(), 50);
        }
    }

    @Test
    public void greaterThanOrEqualTo()
    {
        VecType[] types = {VecType.INT, VecType.INT};
        int[] projectIndices = {1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN_OR_EQUAL(#1, 30)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 5000;
        IntVec col1 =  new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i);
            int v = (i * (i + 2)) % 40;
            if (i % 45 == 0) v = 30;
            col2.set(i, v);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1, col2));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 834);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res0.hasRemaining()) {
            Assert.assertTrue(res0.get() >= 30);
        }
    }

    @Test(enabled = false)
    public void notEqualTo()
    {
        VecType[] types = {VecType.DOUBLE};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$NOT_EQUAL(#0, 0)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 5000;
        DoubleVec col1 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 4999);
        DoubleBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        double cnt = 1;
        while (res0.hasRemaining()) {
            Assert.assertEquals(res0.get(), cnt++);
        }
    }

    @Test
    public void allPass()
    {
        VecType[] types = {VecType.INT};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$EQUAL(#0, 9348)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 20000;
        IntVec col1 = new IntVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, 9348);
        }
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 20000);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res0.hasRemaining()) {
            Assert.assertEquals(res0.get(), 9348);
        }
    }

    @Test
    public void multipleInputs()
    {
        VecType[] types = {VecType.INT};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN_OR_EQUAL(#0, 4)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 1000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 10);
            col2.set(i, i % 6 + 1);
        }
        op.addInput(makeInput(NUM_ROWS, col1));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 500);

        IntBuffer res1 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res1.hasRemaining()) {
            Assert.assertTrue(res1.get() <= 4);
        }

        // Test multiple inputs
        op.addInput(makeInput(NUM_ROWS, col2));
        Assert.assertTrue(op.getOutput().hasNext());
	    res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 668);
        res1 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res1.hasRemaining()) {
            Assert.assertTrue(res1.get() <= 4);
        }
        op.close();
    }

    @Test(enabled=false)
    public void negativeValues() {

        VecType[] types = {VecType.INT, VecType.LONG};
        int[] projectIndices = {0, 1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND($operator$LESS_THAN_OR_EQUAL(#0, -1), $operator$LESS_THAN_OR_EQUAL(#1, -1))",
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        LongVec col2 = new LongVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            int val1 = i * i * 100 + 1;
            if (i % 5 == 0) val1 = -val1;
            col1.set(i, val1);
            long val2 = i % 100 + (long) 3e9;
            if (i % 7 == 0) val2 = -val2;
            col2.set(i, val2);
        }

        op.addInput(makeInput(NUM_ROWS, col1, col2));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 286);
        IntBuffer res1 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        LongBuffer res2 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        while (res1.hasRemaining()) {
            Assert.assertTrue(res1.get() < 0);
            Assert.assertTrue(res2.get() < 0);
        }

    }

    @Test(enabled=false)
    public void allTypes() {
        VecType[] types = {VecType.INT, VecType.LONG, VecType.DOUBLE};
        int[] projectIndices = {0, 1, 2};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND($operator$EQUAL(#0, 0), AND($operator$EQUAL(#1, 1000000000), $operator$GREATER_THAN_OR_EQUAL(#2, 0.4)))",
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        LongVec col2 = new LongVec(NUM_ROWS);
        DoubleVec col3 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 3);
            col2.set(i, i % 2 == 0 ? (long) 3e9 : 0);
            col3.set(i, i % 10 / 10D);
        }

        op.addInput(makeInput(NUM_ROWS, col1, col2, col3));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 1000);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        LongBuffer res1 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        DoubleBuffer res2 = res.getVectors()[2].getData().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        while (res1.hasRemaining()) {
            Assert.assertEquals(res0.get(), 0);
            Assert.assertEquals(res1.get(), (long) 3e9);
            Assert.assertTrue(res2.get() >= 0.4);
        }
    }

    @Test(enabled=false)
    public void compileTest() {
        VecType[] types = {VecType.INT, VecType.INT, VecType.DOUBLE, VecType.DOUBLE};
        int[] projectIndices = {0};
        final int NUM_ROWS = 1000;
        IntVec col1 = new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        DoubleVec col3 = new DoubleVec(NUM_ROWS);
        DoubleVec col4 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 26);
            col2.set(i, 6);
            col3.set(i, i % 10 / 100D);
            col4.set(i, i);
        }

        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND(AND($operator$GREATER_THAN(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND($operator$BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))",
                types,
                projectIndices
        );
        OmniOperator op = factory.createOperator();
        op.addInput(makeInput(NUM_ROWS, col1, col2, col3, col4));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 100);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res0.hasRemaining()) {
            Assert.assertTrue(res0.get() < 24);
        }
    }

    @Test(enabled=false)
    public void logicalOperators1() {
        VecType[] types = {VecType.INT, VecType.INT, VecType.INT, VecType.LONG, VecType.DOUBLE, VecType.LONG};
        int[] projectIndices = {0, 2, 4, 5};
        String s = "OR($operator$GREATER_THAN_OR_EQUAL(#5, 52), AND($operator$LESS_THAN(#4, 50.8), AND(AND($operator$GREATER_THAN(#2, 4800), $operator$LESS_THAN_OR_EQUAL(#1, 9990)), AND($operator$NOT_EQUAL(#0, 1), $operator$EQUAL(#3, 3000000000)))))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                s,
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        IntVec col3 = new IntVec(NUM_ROWS);
        LongVec col4 = new LongVec(NUM_ROWS);
        DoubleVec col5 =  new DoubleVec(NUM_ROWS);
        LongVec col6 = new LongVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 3 == 0 ? 0 : 1);
            col2.set(i, i);
            col3.set(i, i);
            col4.set(i, i % 2 == 0 ? 3000000000L : 2999999999L);
            col5.set(i, 50 + i / 10D);
            col6.set(i, i % 55);
        }

        op.addInput(makeInput(NUM_ROWS, col1, col2, col3, col4, col5, col6));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 543);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        IntBuffer res2 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        DoubleBuffer res4 = res.getVectors()[2].getData().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        LongBuffer res5 = res.getVectors()[3].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        while (res0.hasRemaining()) {
            Assert.assertTrue((res0.get() != 1 && res2.get() > 4800 && res4.get() < 50.8) ||  res5.get() >= 52);
        }

    }

    @Test(enabled=false)
    public void logicalOperators2() {
        VecType[] types = {VecType.INT, VecType.INT, VecType.LONG, VecType.LONG};
        int[] projectIndices = {3, 2, 1, 0};
        String s = "AND(OR($operator$LESS_THAN(#0, 50), $operator$EQUAL(#1, -12)), OR($operator$LESS_THAN_OR_EQUAL(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL(#3, 0)))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                s,
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        LongVec col3 = new LongVec(NUM_ROWS);
        LongVec col4 = new LongVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 100);
            col2.set(i, i % 7 == 0 ? -12 : i);
            col3.set(i, i % 8 == 0 ? -i - 3000000000L : i + 3000000000L);
            col4.set(i, i % 9 - 4);
        }

        op.addInput(makeInput(NUM_ROWS, col1, col2, col3, col4));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 3498);
        LongBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        LongBuffer res1 = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        IntBuffer res2 = res.getVectors()[2].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        IntBuffer res3 = res.getVectors()[3].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res0.hasRemaining()) {
            Assert.assertTrue((res0.get() >= 0 || res1.get() <= -3e9) && (res2.get() == -12 || res3.get() < 50));
        }
    }

    @Test(enabled=false)
    public void logicalOperators3() {
        VecType[] types = {VecType.INT, VecType.DOUBLE};
        int[] projectIndices = {1, 0};
        String expr = "OR(OR(OR($operator$EQUAL(#0, 1), $operator$EQUAL(#0, 2)), $operator$EQUAL(#0, 3)), OR(OR(OR(OR($operator$EQUAL(#0, 999), $operator$EQUAL(#0, 5)), $operator$EQUAL(#0, 8)), $operator$EQUAL(#0, 13)), $operator$NOT_EQUAL(#1, 0)))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                expr,
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(NUM_ROWS);
        DoubleVec col2 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, 0);
            col2.set(i, 1.5);
        }
        col1.set(0, 0);
        col1.set(1, 1);
        col1.set(2, 1);
        col1.set(3, 2);
        col1.set(4, 3);
        col1.set(5, 5);
        col1.set(6, 8);
        col1.set(7, 13);
        col2.set(2, 0);

        op.addInput(makeInput(NUM_ROWS, col1, col2));

        Assert.assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 6);
        IntBuffer fib = res.getVectors()[1].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        Assert.assertEquals(fib.get(), 1);
        Assert.assertEquals(fib.get(), 2);
        Assert.assertEquals(fib.get(), 3);
        Assert.assertEquals(fib.get(), 5);
        Assert.assertEquals(fib.get(), 8);
        Assert.assertEquals(fib.get(), 13);
    }

    @Test(enabled=false)
    public void arithmeticAdd()
    {
        VecType[] types = {VecType.INT};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN(ADD(#0, 1), 4)",
                types,
                projectIndices
        );
        final int NUM_ROWS = 10000;
        IntVec col1 = new IntVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i % 5);
        }

        OmniOperator op = factory.createOperator();

        Assert.assertTrue(op.getOutput().hasNext());
        op.addInput(makeInput(NUM_ROWS, col1));
        VecBatch res = op.getOutput().next();
        Assert.assertEquals(res.getRowCount(), 2000);
        IntBuffer res0 = res.getVectors()[0].getData().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        while (res0.hasRemaining()) {
            Assert.assertEquals(res0.get(), 5);
        }
    }


    private List<Vec> createTable(final int NUM_ROWS)
    {
        IntVec col1 = new IntVec(NUM_ROWS);
        IntVec col2 = new IntVec(NUM_ROWS);
        DoubleVec col3 = new DoubleVec(NUM_ROWS);
        DoubleVec col4 = new DoubleVec(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++) {
            col1.set(i, i);
            col2.set(i, i);
            col3.set(i, i);
            col4.set(i, i);
        }
        List<Vec> table = new ArrayList<>();
        table.add(col1);
        table.add(col2);
        table.add(col3);
        table.add(col4);
        return table;
    }

    @Test(enabled=false)
    public void multithreadTest()
    {
        VecType[] types = {VecType.INT, VecType.INT, VecType.DOUBLE, VecType.DOUBLE};
        int[] projectIndices = {0, 1, 2, 3};
        String s = "$operator$LESS_THAN_OR_EQUAL(#0, 500)";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                s,
                types,
                projectIndices
        );
        final int NUM_ROWS = 1000;
        OmniOperator op = factory.createOperator();
        List<Vec> table = createTable(NUM_ROWS);
        for (int i = 0; i < 1000; i++) {
            Thread t = new Thread(() -> {
                op.addInput(ImmutableList.copyOf(new VecBatch[] {new VecBatch(table, NUM_ROWS)}));
                Assert.assertTrue(op.getOutput().hasNext());
                VecBatch res = op.getOutput().next();
                // System.out.println(res.getLength());
                Assert.assertEquals(res.getRowCount(), 501);
            });
            t.start();
        }
        try {
            // Wait for all to finish
            Thread.sleep(10000);
        }
        catch (Exception e) {}
    }
    
}
