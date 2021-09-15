package nova.hetu.omniruntime.operator;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.testng.Assert.*;

/**
 * The type Omni project operator test.
 */
public class OmniProjectOperatorTest {
    private ImmutableList<VecBatch> makeInput(int nRows, Vec... cols) {
        return ImmutableList.copyOf(new VecBatch[] {new VecBatch(cols)});
    }

    /**
     * Simple test.
     */
    @Test
    public void simpleTest() {
        String[] exprs = {"$operator$ADD:int(#0, 5)"};
        VecType[] inputTypes = {IntVecType.INTEGER};
        OmniProjectOperatorFactory factory = new OmniProjectOperatorFactory(exprs, inputTypes);
        final int numRows = 1000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1)) {
            op.addInput(vecBatch);
        }

        Iterator<VecBatch> vecBatchIterator = op.getOutput();
        assertTrue(vecBatchIterator.hasNext());
        VecBatch res = op.getOutput().next();
        assertFalse(vecBatchIterator.hasNext());
        assertEquals(res.getRowCount(), numRows);
        IntBuffer res1 = res.getVectors()[0].getValues().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        for (int i = 0; i < numRows; i++) {
            assertEquals(res1.get(), i + 5);
        }
    }

    /**
     * Complex test.
     */
    @Test
    public void complexTest() {
        String[] exprs = {"$operator$MULTIPLY:int(#0, #1)", "IF:long($operator$LESS_THAN:boolean(#0, 500), 4000000000, #2)"};
        VecType[] inputTypes = {IntVecType.INTEGER, IntVecType.INTEGER, LongVecType.LONG};
        OmniProjectOperatorFactory factory = new OmniProjectOperatorFactory(exprs, inputTypes);
        final int numRows = 1000;
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        LongVec col3 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i + 1);
            col2.set(i, i - 100);
            col3.set(i, i + 3000000000L);
        }
        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3)) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), numRows);
        IntBuffer res1 = res.getVectors()[0].getValues().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        LongBuffer res2 = res.getVectors()[1].getValues().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        for (int i = 0; i < numRows; i++) {
            assertEquals(res1.get(), (i + 1) * (i - 100));
            assertEquals(res2.get(), (i + 1) < 500 ? 4000000000L : i + 3000000000L);
        }
    }

    /**
     * Murmur3hash test.
     */
    @Test
    public void mm3HashTest() {
        String[] exprs = {"mm3hash:int(#0, 42)", "mm3hash:int(#1, 42)", "mm3hash:int(#2, 42)"};
        VecType[] inputTypes = {IntVecType.INTEGER, DoubleVecType.DOUBLE, VarcharVecType.VARCHAR};
        OmniProjectOperatorFactory factory = new OmniProjectOperatorFactory(exprs, inputTypes);
        final int numRows = 1;
        final byte[] byteVal = "Wednesday".getBytes(StandardCharsets.UTF_8);
        IntVec col1 = new IntVec(numRows);
        DoubleVec col2 = new DoubleVec(numRows);
        VarcharVec col3 = new VarcharVec(byteVal.length, numRows);

        col1.set(0, Integer.MIN_VALUE);
        col2.set(0, Double.MAX_VALUE);
        col3.set(0, byteVal);

        OmniOperator op = factory.createOperator();
        for (VecBatch vecBatch : makeInput(numRows, col1, col2, col3)) {
            op.addInput(vecBatch);
        }
        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), numRows);
        assertEquals(res.getVectors().length, exprs.length);
        IntBuffer res1 = res.getVectors()[0].getValues().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        IntBuffer res2 = res.getVectors()[1].getValues().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        IntBuffer res3 = res.getVectors()[2].getValues().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        assertEquals(res1.get(), 723455942);
        assertEquals(res2.get(), -508695674);
        assertEquals(res3.get(), 613818021);
    }
}
