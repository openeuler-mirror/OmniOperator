package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

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
        String[] exprs = {"$operator$ADD(#0, 5)"};
        VecType[] inputTypes = {OMNI_VEC_TYPE_INT};
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

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
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
        String[] exprs = {"$operator$MULTIPLY(#0, #1)", "IF($operator$LESS_THAN(#0, 500), 4000000000, #2)"};
        VecType[] inputTypes = {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_LONG};
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
}
