package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import static nova.hetu.omniruntime.operator.JFilterAndProjectOperator.JFilterAndProjectOperatorFactory.create;

public class JFilterAndProjectTest {

    @Test
    public void basicFilterTest()
    {
        // Tests the placeholder filter which gets all rows with an even number in the first column.
        VecType[] types = {VecType.INT, VecType.INT, VecType.DOUBLE, VecType.DOUBLE};
        int[] projectIndices = {0, 1, 2, 3};
        String s = "AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))";
        JFilterAndProjectOperator.JFilterAndProjectOperatorFactory factory = create(
                s,
                types,
                projectIndices
        );
        final int NUM_ROWS = 1000;
        JOmniOperator op = factory.createOmniOperator();
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
        op.addInput(table, 1000);

        OMResult res = op.getOutput()[0];
        Assert.assertEquals(res.getLength(), 500);
        ByteBuffer[] buffers = res.getBuffers();

        IntBuffer res1 = buffers[0].order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int cnt = 0;
        while (res1.hasRemaining()) {
            Assert.assertEquals(res1.get(), cnt);
            cnt += 2;
        }
    }
    
}