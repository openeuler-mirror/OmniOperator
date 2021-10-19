package nova.hetu.omniruntime.operator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.JvmUtils;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The type Omni filter and project operator test.
 */
public class OmniFilterAndProjectOperatorTest {
    private ImmutableList<VecBatch> makeInput(int nRows, Vec... cols) {
        return ImmutableList.copyOf(new VecBatch[] {new VecBatch(cols)});
    }

    /**
     * Doubles.
     */
    @Test
    public void doubles() {
        VecType[] types = {DoubleVecType.DOUBLE};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$LESS_THAN:boolean(#0, 1.0)", types, projectIndices);
        final int numRows = 5000;
        DoubleVec col1 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 2 == 0 ? 0.5 : 1.5);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 2500);
        DoubleBuffer res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asDoubleBuffer();
        while (res1.hasRemaining()) {
            assertTrue(res1.get() < 1);
        }
    }

    /**
     * Less than.
     */
    @Test
    public void lessThan() {
        VecType[] types = {IntVecType.INTEGER};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$LESS_THAN:boolean(#0, 2000)", types, projectIndices);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
        IntBuffer res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res1.hasRemaining()) {
            assertTrue(res1.get() < 2000);
        }
    }

    /**
     * Greater than.
     */
    @Test
    public void greaterThan() {
        VecType[] types = {IntVecType.INTEGER, LongVecType.LONG};
        int[] projectIndices = {0, 1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$GREATER_THAN:boolean(#0, 20)", types, projectIndices);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 25);
            col2.set(i, 3000000000L);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 800);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        LongBuffer res1 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asLongBuffer();
        while (res0.hasRemaining()) {
            assertTrue(res0.get() > 20);
            assertEquals(res1.get(), 3000000000L);
        }
    }

    /**
     * Equal to.
     */
    @Test
    public void equalTo() {
        VecType[] types = {IntVecType.INTEGER, LongVecType.LONG, DoubleVecType.DOUBLE};
        int[] projectIndices = {1, 2};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory("$operator$EQUAL:boolean(#1, 50)",
            types, projectIndices);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, i % 100);
            col3.set(i, i % 100);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 50);
        DoubleBuffer res0 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asDoubleBuffer();
        LongBuffer res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asLongBuffer();
        while (res0.hasRemaining()) {
            assertEquals(res0.get(), 50.0);
            assertEquals(res1.get(), 50);
        }
    }

    /**
     * Greater than or equal to.
     */
    @Test
    public void greaterThanOrEqualTo() {
        VecType[] types = {IntVecType.INTEGER, IntVecType.INTEGER};
        int[] projectIndices = {1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$GREATER_THAN_OR_EQUAL:boolean(#1, 30)", types, projectIndices);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
            int value = (i * (i + 2)) % 40;
            if (i % 45 == 0) {
                value = 30;
            }
            col2.set(i, value);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 834);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res0.hasRemaining()) {
            assertTrue(res0.get() >= 30);
        }
    }

    /**
     * Not equal to.
     */
    @Test
    public void notEqualTo() {
        VecType[] types = {DoubleVecType.DOUBLE};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$NOT_EQUAL:boolean(#0, 0)", types, projectIndices);
        final int numRows = 5000;
        DoubleVec col1 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 4999);
        DoubleBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asDoubleBuffer();
        double cnt = 1;
        while (res0.hasRemaining()) {
            assertEquals(res0.get(), cnt++);
        }
    }

    /**
     * All pass.
     */
    @Test
    public void allPass() {
        VecType[] types = {IntVecType.INTEGER};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$EQUAL:boolean(#0, 9348)", types, projectIndices);
        final int numRows = 20000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, 9348);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 20000);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res0.hasRemaining()) {
            assertEquals(res0.get(), 9348);
        }
    }

    /**
     * Multiple inputs.
     */
    @Test
    public void multipleInputs() {
        VecType[] types = {IntVecType.INTEGER};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$LESS_THAN_OR_EQUAL:boolean(#0, 4)", types, projectIndices);
        final int numRows = 1000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 10);
            col2.set(i, i % 6 + 1);
        }
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 500);

        IntBuffer res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res1.hasRemaining()) {
            assertTrue(res1.get() <= 4);
        }

        // Test multiple inputs
        for (VecBatch vecBatch : makeInput(numRows, col2)) {
            op.addInput(vecBatch);
        }
        assertTrue(op.getOutput().hasNext());
        res = op.getOutput().next();
        assertEquals(res.getRowCount(), 668);
        res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res1.hasRemaining()) {
            assertTrue(res1.get() <= 4);
        }
        op.close();
    }

    /**
     * Negative values.
     */
    @Test
    public void negativeValues() {
        VecType[] types = {IntVecType.INTEGER, LongVecType.LONG};
        int[] projectIndices = {0, 1};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "AND:boolean($operator$LESS_THAN_OR_EQUAL:boolean(#0, -1), $operator$LESS_THAN_OR_EQUAL:boolean(#1, -1))", types, projectIndices);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            int val1 = i * i + 1;
            if (i % 5 == 0) {
                val1 = -val1;
            }
            col1.set(i, val1);
            long val2 = i % 100 + (long) 3e9;
            if (i % 7 == 0) {
                val2 = -val2;
            }
            col2.set(i, val2);
        }

        for (VecBatch vecBatch : makeInput(numRows, col1, col2)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 286);
        IntBuffer res1 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        LongBuffer res2 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asLongBuffer();
        while (res1.hasRemaining()) {
            assertTrue(res1.get() < 0);
            assertTrue(res2.get() < 0);
        }
    }

    /**
     * All types.
     */
    @Test(enabled = false)
    public void allTypes() {
        VecType[] types = {IntVecType.INTEGER, LongVecType.LONG, DoubleVecType.DOUBLE};
        int[] projectIndices = {0, 1, 2};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "AND:boolean($operator$EQUAL:boolean(#0, 0), AND:boolean($operator$EQUAL:boolean(#1, 3000000000), " +
                    "$operator$GREATER_THAN_OR_EQUAL:boolean(#2, 0.4)))",
            types, projectIndices);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 3);
            col2.set(i, i % 2 == 0 ? (long) 3e9 : 0);
            col3.set(i, i % 10 / 10D);
        }

        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 1000);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        LongBuffer res1 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asLongBuffer();
        DoubleBuffer res2 = JvmUtils.directBuffer(res.getVectors()[2].getValuesBuf()).asDoubleBuffer();
        while (res1.hasRemaining()) {
            assertEquals(res0.get(), 0);
            assertEquals(res1.get(), (long) 3e9);
            assertTrue(res2.get() >= 0.4);
        }
    }

    /**
     * Compile test.
     */
    @Test(enabled = false)
    public void compileTest() {
        VecType[] types = {IntVecType.INTEGER, IntVecType.INTEGER, DoubleVecType.DOUBLE, DoubleVecType.DOUBLE};
        int[] projectIndices = {0};
        final int numRows = 1000;
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        DoubleVec col4 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 26);
            col2.set(i, 6);
            col3.set(i, i % 10 / 100D);
            col4.set(i, i);
        }

        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "AND:boolean(AND:boolean($operator$GREATER_THAN:boolean(#3, 8766), $operator$LESS_THAN:boolean(#3, 9131)), " +
                    "AND:boolean($operator$BETWEEN:boolean(#2, 0.05, 0.07), $operator$LESS_THAN:boolean(#0, 24.0)))",
            types, projectIndices);
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3, col4)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 100);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res0.hasRemaining()) {
            assertTrue(res0.get() < 24);
        }
    }

    /**
     * Logical operators 1.
     */
    @Test
    public void logicalOperators1() {
        VecType[] types = {
            IntVecType.INTEGER, IntVecType.INTEGER, IntVecType.INTEGER, LongVecType.LONG, DoubleVecType.DOUBLE,
            LongVecType.LONG
        };
        int[] projectIndices = {0, 2, 4, 5};
        String str
            = "OR:boolean($operator$GREATER_THAN_OR_EQUAL:boolean(#5, 52), AND:boolean($operator$LESS_THAN:boolean(#4, 50.8), AND:boolean(AND:boolean($operator$GREATER_THAN:boolean(#2, 4800), " +
                "$operator$LESS_THAN_OR_EQUAL:boolean(#1, 9990)), AND:boolean($operator$NOT_EQUAL:boolean(#0, 1), $operator$EQUAL:boolean(#3, 3000000000)))))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projectIndices);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        IntVec col3 = new IntVec(numRows);
        LongVec col4 = new LongVec(numRows);
        DoubleVec col5 = new DoubleVec(numRows);
        LongVec col6 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 3 == 0 ? 0 : 1);
            col2.set(i, i);
            col3.set(i, i);
            col4.set(i, i % 2 == 0 ? 3000000000L : 2999999999L);
            col5.set(i, 50 + i / 10D);
            col6.set(i, i % 55);
        }

        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3, col4, col5, col6)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 543);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        IntBuffer res2 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asIntBuffer();
        DoubleBuffer res4 = JvmUtils.directBuffer(res.getVectors()[2].getValuesBuf()).asDoubleBuffer();
        LongBuffer res5 = JvmUtils.directBuffer(res.getVectors()[3].getValuesBuf()).asLongBuffer();
        while (res0.hasRemaining()) {
            assertTrue((res0.get() != 1 && res2.get() > 4800 && res4.get() < 50.8) || res5.get() >= 52);
        }
    }

    /**
     * Logical operators 2.
     */
    @Test
    public void logicalOperators2() {
        VecType[] types = {IntVecType.INTEGER, IntVecType.INTEGER, LongVecType.LONG, LongVecType.LONG};
        int[] projectIndices = {3, 2, 1, 0};
        String str
            = "AND:boolean(OR:boolean($operator$LESS_THAN:boolean(#0, 50), $operator$EQUAL:boolean(#1, -12)), " +
                "OR:boolean($operator$LESS_THAN_OR_EQUAL:boolean(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL:boolean(#3, 0)))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projectIndices);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        LongVec col3 = new LongVec(numRows);
        LongVec col4 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 100);
            col2.set(i, i % 7 == 0 ? -12 : i);
            col3.set(i, i % 8 == 0 ? -i - 3000000000L : i + 3000000000L);
            col4.set(i, i % 9 - 4);
        }

        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3, col4)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 3498);
        LongBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asLongBuffer();
        LongBuffer res1 = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asLongBuffer();
        IntBuffer res2 = JvmUtils.directBuffer(res.getVectors()[2].getValuesBuf()).asIntBuffer();
        IntBuffer res3 = JvmUtils.directBuffer(res.getVectors()[3].getValuesBuf()).asIntBuffer();
        while (res0.hasRemaining()) {
            long v0 = res0.get();
            long v1 = res1.get();
            int v2 = res2.get();
            int v3 = res3.get();
            assertTrue((v0 >= 0 || v1 <= -3000000000L) && (v2 == -12 || v3 < 50));
        }
    }

    /**
     * Logical operators 3.
     */
    @Test
    public void logicalOperators3() {
        VecType[] types = {IntVecType.INTEGER, DoubleVecType.DOUBLE};
        int[] projectIndices = {1, 0};
        String expr
            = "AND:boolean($operator$NOT_EQUAL:boolean(#1, 0), OR:boolean(OR:boolean(OR:boolean($operator$EQUAL:boolean(#0, 1), " +
                "$operator$EQUAL:boolean(#0, 2)), $operator$EQUAL:boolean(#0, 3)), OR:boolean(OR:boolean(OR:boolean($operator$EQUAL:boolean(55, #0), " +
                "$operator$EQUAL:boolean(5, #0)), $operator$EQUAL:boolean(#0, 8)), $operator$EQUAL:boolean(#0, 13))))";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(expr, types,
            projectIndices);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        IntVec col1 = new IntVec(numRows);
        DoubleVec col2 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
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

        for (VecBatch vecBatch : makeInput(numRows, col1, col2)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 6);
        IntBuffer fib = JvmUtils.directBuffer(res.getVectors()[1].getValuesBuf()).asIntBuffer();
        assertEquals(fib.get(), 1);
        assertEquals(fib.get(), 2);
        assertEquals(fib.get(), 3);
        assertEquals(fib.get(), 5);
        assertEquals(fib.get(), 8);
        assertEquals(fib.get(), 13);
    }

    /**
     * Arithmetic add.
     */
    @Test
    public void arithmeticAdd() {
        VecType[] types = {IntVecType.INTEGER};
        int[] projectIndices = {0};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "$operator$GREATER_THAN:boolean(ADD:int(#0, 1), 4)", types, projectIndices);
        final int numRows = 10000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 5);
        }

        OmniOperator op = factory.createOperator();

        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }
        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
        IntBuffer res0 = JvmUtils.directBuffer(res.getVectors()[0].getValuesBuf()).asIntBuffer();
        while (res0.hasRemaining()) {
            assertTrue(res0.get() + 1 > 4);
        }
    }

    private List<Vec> createTable(final int numRows) {
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        DoubleVec col4 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
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

    /**
     * Multithread test.
     */
    @Test
    public void multithreadTest() {
        VecType[] types = {IntVecType.INTEGER, IntVecType.INTEGER, DoubleVecType.DOUBLE, DoubleVecType.DOUBLE};
        int[] projectIndices = {0, 1, 2, 3};
        String str = "$operator$LESS_THAN_OR_EQUAL:boolean(#0, 500)";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projectIndices);
        final int numRows = 1000;
        for (int i = 0; i < 1000; i++) {
            Thread thread = new Thread(() -> {
                OmniOperator op = factory.createOperator();
                op.addInput(new VecBatch(createTable(numRows)));
                assertTrue(op.getOutput().hasNext());
                VecBatch res = op.getOutput().next();
                // System.out.println(res.getLength());
                assertEquals(res.getRowCount(), 501);
            });
            thread.setName("thread"+i);
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread thread1, Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
            thread.start();
        }
        try {
            // Wait for all to finish
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Conditional.
     */
    @Test
    public void conditional() {
        VecType[] types = {IntVecType.INTEGER, IntVecType.INTEGER, IntVecType.INTEGER};
        int[] projectIndices = {0, 1, 2};
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
            "AND:boolean(IF:boolean($operator$EQUAL:boolean(#0, 0), $operator$LESS_THAN:boolean(#1, 3), " +
                    "$operator$EQUAL:boolean(#1, 4)), $operator$GREATER_THAN:boolean(#2, 3))",
            types, projectIndices);
        final int numRows = 10000;
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        IntVec col3 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 2);
            col2.set(i, i % 5);
            col3.set(i, i % 10);
        }

        OmniOperator op = factory.createOperator();

        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3)) {
            op.addInput(vecBatch);
        }
        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
    }
}
