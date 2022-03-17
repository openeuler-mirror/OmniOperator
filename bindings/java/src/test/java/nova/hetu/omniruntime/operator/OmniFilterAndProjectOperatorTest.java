
package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createIntVec;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.util.TestUtils;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni filter and project operator test.
 */
public class OmniFilterAndProjectOperatorTest {
    private ImmutableList<VecBatch> makeInput(int nRows, Vec... cols) {
        return ImmutableList.copyOf(new VecBatch[]{new VecBatch(cols)});
    }

    /**
     * Between int.
     */
    @Test
    public void betweenInt() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0", "#1", "#2");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$BETWEEN:4(#1, #0, #2)", types, projections);

        Object[][] datas = {{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                {0, 1, 2, 3, 4, 5, 6, 6, 6, 6}};
        VecBatch vecBatch = createVecBatch(types, datas);
        OmniOperator op = factory.createOperator();
        op.addInput(vecBatch);

        Iterator<VecBatch> results = op.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectDatas = {{0, 1, 2, 3, 4, 0, 1}, {0, 1, 2, 3, 4, 5, 6}, {0, 1, 2, 3, 4, 5, 6}};
        assertVecBatchEquals(resultVecBatch, expectDatas);

        freeVecBatch(resultVecBatch);
        op.close();
        factory.close();
    }

    /**
     * Between int dictionary.
     */
    @Test
    public void betweenIntDictionary() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0", "#1", "#2");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$BETWEEN:4(#1, #0, #2)", types, projections);

        Object[][] datas = {{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                {-3, -2, -1, 0, 1, 2, 3, 4, 5, 6}};
        Vec vecs[] = new Vec[3];
        vecs[0] = TestUtils.createIntVec(datas[0]);
        vecs[1] = TestUtils.createIntVec(datas[1]);

        int[] ids = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
        DictionaryVec dicVec = TestUtils.createDictionaryVec(types[2], datas[2], ids);
        dicVec = new DictionaryVec(dicVec, ids);
        vecs[2] = dicVec;

        VecBatch vecBatch = new VecBatch(vecs);
        OmniOperator op = factory.createOperator();
        op.addInput(vecBatch);

        Iterator<VecBatch> results = op.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectDatas = {{0, 1, 2, 3, 4, 0, 1}, {0, 1, 2, 3, 4, 5, 6}, {3, 4, 5, 6, 6, 6, 6}};
        assertVecBatchEquals(resultVecBatch, expectDatas);

        freeVecBatch(resultVecBatch);
        op.close();
        factory.close();
    }

    /**
     * Doubles.
     */
    @Test
    public void doubles() {
        DataType[] types = {DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN:4(#0, 1.0:3)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0},\"right\"" +
            ":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":1.0}}",
            types, projectionsJSON, 1);
        final int numRows = 5000;
        DoubleVec col1 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 2 == 0 ? 0.5 : 1.5);
        }
        DoubleVec col2 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, i % 2 == 0 ? 0.5 : 1.5);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col2);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 2500);
        assertEquals(resJSON.getRowCount(), 2500);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((DoubleVec) res.getVector(0)).get(i) < 1);
            assertTrue(((DoubleVec) resJSON.getVector(0)).get(i) < 1);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Less than.
     */
    @Test
    public void lessThan() {
        DataType[] types = {IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN:4(#0, 2000:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":2000}}",
            types, projectionsJSON, 1);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
        }
        IntVec col2 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col2);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
        assertEquals(resJSON.getRowCount(), 2000);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) < 2000);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) < 2000);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Less than dictionary varchar.
     */
    @Test
    public void lessThanDictionaryVarchar() {
        DataType[] types = {IntDataType.INTEGER, new VarcharDataType(50)};
        List<String> projections = ImmutableList.of("#0", "#1");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN:4(#0, 6:1)", types, projections);

        Object[][] datas = {{0, 3, 9}, {"hello", "world", "friends"}};
        Vec vecs[] = new Vec[2];
        vecs[0] = createIntVec(datas[0]);
        int[] ids = {0, 1, 2};
        DictionaryVec dicVec = TestUtils.createDictionaryVec(types[1], datas[1], ids);
        vecs[1] = dicVec;
        VecBatch vecBatch = new VecBatch(vecs);

        OmniOperator op = factory.createOperator();
        op.addInput(vecBatch);

        Iterator<VecBatch> results = op.getOutput();
        VecBatch resultVecBatch = results.next();

        Object[][] expectDatas = {{0, 3}, {"hello", "world"}};
        assertVecBatchEquals(resultVecBatch, expectDatas);

        freeVecBatch(resultVecBatch);
        op.close();
        factory.close();
    }

    /**
     * Greater than.
     */
    @Test
    public void greaterThan() {
        DataType[] types = {IntDataType.INTEGER, LongDataType.LONG};
        List<String> projections = ImmutableList.of("#0", "#1");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN:4(#0, 20:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":20}}",
            types, projectionsJSON, 1);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 25);
            col2.set(i, 3000000000L);
        }
        IntVec col3 = new IntVec(numRows);
        LongVec col4 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col3.set(i, i % 25);
            col4.set(i, 3000000000L);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col3, col4);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 800);
        assertEquals(resJSON.getRowCount(), 800);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) > 20);
            assertEquals(((LongVec) res.getVector(1)).get(i), 3000000000L);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) > 20);
            assertEquals(((LongVec) resJSON.getVector(1)).get(i), 3000000000L);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Equal to.
     */
    @Test
    public void equalTo() {
        DataType[] types = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#1", "#2");
        List<String> projectionsJSON =
                        ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}",
                                "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$EQUAL:4(#1, 50:2)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":50}}",
            types, projectionsJSON, 1);
        final int numRows = 5000;
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, i % 100);
            col3.set(i, i % 100);
        }
        IntVec col4 = new IntVec(numRows);
        LongVec col5 = new LongVec(numRows);
        DoubleVec col6 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col5.set(i, i % 100);
            col6.set(i, i % 100);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2, col3);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col4, col5, col6);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 50);
        assertEquals(resJSON.getRowCount(), 50);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertEquals(((LongVec) res.getVector(0)).get(i), 50);
            assertEquals(((DoubleVec) res.getVector(1)).get(i), 50.0);
            assertEquals(((LongVec) resJSON.getVector(0)).get(i), 50);
            assertEquals(((DoubleVec) resJSON.getVector(1)).get(i), 50.0);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Greater than or equal to.
     */
    @Test
    public void greaterThanOrEqualTo() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#1");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN_OR_EQUAL:4(#1, 30:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":" +
            "\"GREATER_THAN_OR_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1," +
            "\"colVal\":1},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":30}}",
            types, projectionsJSON, 1);
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
        IntVec col3 = new IntVec(numRows);
        IntVec col4 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col3.set(i, i);
            int value = (i * (i + 2)) % 40;
            if (i % 45 == 0) {
                value = 30;
            }
            col4.set(i, value);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col3, col4);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 834);
        assertEquals(resJSON.getRowCount(), 834);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) >= 30);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) >= 30);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Not equal to.
     */
    @Test
    public void notEqualTo() {
        DataType[] types = {DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$NOT_EQUAL:4(#0, 0:3)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":0}}",
            types, projectionsJSON, 1);
        final int numRows = 5000;
        DoubleVec col1 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i);
        }
        DoubleVec col2 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, i);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col2);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 4999);
        assertEquals(resJSON.getRowCount(), 4999);
        double cnt = 1;
        for (int i = 0; i < res.getRowCount(); i++) {
            assertEquals(((DoubleVec) res.getVector(0)).get(i), cnt);
            assertEquals(((DoubleVec) resJSON.getVector(0)).get(i), cnt);
            cnt++;
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * All pass.
     */
    @Test
    public void allPass() {
        DataType[] types = {IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$EQUAL:4(#0, 9348:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":9348}}",
            types, projectionsJSON, 1);
        final int numRows = 20000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, 9348);
        }
        IntVec col2 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col2.set(i, 9348);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col2);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 20000);
        assertEquals(resJSON.getRowCount(), 20000);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertEquals(((IntVec) res.getVector(0)).get(i), 9348);
            assertEquals(((IntVec) resJSON.getVector(0)).get(i), 9348);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Multiple inputs.
     */
    @Test
    public void multipleInputs() {
        DataType[] types = {IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$LESS_THAN_OR_EQUAL:4(#0, 4:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN_OR_EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":4}}",
            types, projectionsJSON, 1);
        final int numRows = 1000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 10);
            col2.set(i, i % 6 + 1);
        }
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        IntVec col3 = new IntVec(numRows);
        IntVec col4 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col3.set(i, i % 10);
            col4.set(i, i % 6 + 1);
        }
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col3);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 500);
        assertEquals(resJSON.getRowCount(), 500);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) <= 4);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) <= 4);
        }

        // Test multiple inputs
        ImmutableList<VecBatch> vecBatches1_ = makeInput(numRows, col2);
        ImmutableList<VecBatch> vecBatches2_ = makeInput(numRows, col4);
        for (VecBatch vecBatch : vecBatches1_) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2_) {
            opJSON.addInput(vecBatch);
        }
        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        res = op.getOutput().next();
        resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 668);
        assertEquals(resJSON.getRowCount(), 668);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) <= 4);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) <= 4);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Negative values.
     */
    @Test
    public void negativeValues() {
        DataType[] types = {IntDataType.INTEGER, LongDataType.LONG};
        List<String> projections = ImmutableList.of("#0", "#1");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND:4($operator$LESS_THAN_OR_EQUAL:4(#0, -1:1), $operator$LESS_THAN_OR_EQUAL:4(#1, -1:2))", types,
                projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
                "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                    "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":-1}}," +
                "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                    "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":-1}}}",
            types, projectionsJSON, 1);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();

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

        IntVec col3 = new IntVec(numRows);
        LongVec col4 = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            int val1 = i * i + 1;
            if (i % 5 == 0) {
                val1 = -val1;
            }
            col3.set(i, val1);
            long val2 = i % 100 + (long) 3e9;
            if (i % 7 == 0) {
                val2 = -val2;
            }
            col4.set(i, val2);
        }

        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col3, col4);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 286);
        assertEquals(resJSON.getRowCount(), 286);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) < 0);
            assertTrue(((LongVec) res.getVector(1)).get(i) < 0);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) < 0);
            assertTrue(((LongVec) resJSON.getVector(1)).get(i) < 0);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * All types.
     */
    @Test(enabled = false)
    public void allTypes() {
        DataType[] types = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#0", "#1", "#2");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND:4($operator$EQUAL:4(#0, 0:1), AND:4($operator$EQUAL:4(#1, 3000000000:2), "
                        + "$operator$GREATER_THAN_OR_EQUAL:4(#2, 0.4:3)))",
                types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
                "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                    "\"operator\":\"EQUAL\"," +
                        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
                        "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":0}}," +
                "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                    "\"operator\":\"AND\"," +
                        "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                            "\"operator\":\"EQUAL\"," +
                                "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1},"+
                                "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false," +
                                    "\"value\":3000000000}}," +
                        "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                            "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
                                "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2},"+
                                "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false," +
                    "\"value\":0.4}}}}",
            types, projectionsJSON, 1);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        IntVec col1 = new IntVec(numRows);
        LongVec col2 = new LongVec(numRows);
        DoubleVec col3 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 3);
            col2.set(i, i % 2 == 0 ? (long) 3e9 : 0);
            col3.set(i, i % 10 / 10D);
        }
        IntVec col4 = new IntVec(numRows);
        LongVec col5 = new LongVec(numRows);
        DoubleVec col6 = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col4.set(i, i % 3);
            col5.set(i, i % 2 == 0 ? (long) 3e9 : 0);
            col6.set(i, i % 10 / 10D);
        }

        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2, col3);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col4, col5, col6);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 1000);
        assertEquals(resJSON.getRowCount(), 1000);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertEquals(((IntVec) res.getVector(0)).get(i), 0);
            assertEquals(((LongVec) res.getVector(1)).get(i), (long) 3e9);
            assertTrue(((DoubleVec) res.getVector(2)).get(i) >= 0.4);
            assertEquals(((IntVec) resJSON.getVector(0)).get(i), 0);
            assertEquals(((LongVec) resJSON.getVector(1)).get(i), (long) 3e9);
            assertTrue(((DoubleVec) resJSON.getVector(2)).get(i) >= 0.4);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Compile test.
     */
    @Test(enabled = false)
    public void compileTest() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, DoubleDataType.DOUBLE, DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#0");
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
                "AND:4(AND:4($operator$GREATER_THAN:4(#3, 8766:3), $operator$LESS_THAN:4(#3, 9131:3)), "
                        + "AND:4($operator$BETWEEN:4(#2, 0.05:3, 0.07:3), $operator$LESS_THAN:4(#0, 24.0:3)))",
                types, projections);
        OmniOperator op = factory.createOperator();
        ImmutableList<VecBatch> vecBatches = makeInput(numRows, col1, col2, col3, col4);
        for (VecBatch vecBatch : vecBatches) {
            op.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        assertEquals(res.getRowCount(), 100);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) < 24);
        }

        freeVecBatch(res);
        op.close();
    }

    /**
     * Logical operators 1.
     */
    @Test
    public void logicalOperators1() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, IntDataType.INTEGER, LongDataType.LONG,
                DoubleDataType.DOUBLE, LongDataType.LONG};
        List<String> projections = ImmutableList.of("#0", "#2", "#4", "#5");
        String str = "OR:4($operator$GREATER_THAN_OR_EQUAL:4(#5, 52:2), AND:4($operator$LESS_THAN:4(#4, 50.8:3), "
                + "AND:4(AND:4($operator$GREATER_THAN:4(#2, 4800:1), $operator$LESS_THAN_OR_EQUAL:4(#1, 9990:1)), "
                + "AND:4($operator$NOT_EQUAL:4(#0, 1:1), $operator$EQUAL:4(#3, 3000000000:2)))))";
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\": 1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\": 1,\"colVal\":2}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\": 3,\"colVal\":4}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\": 2,\"colVal\":5}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projections);
        String strJSON =
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":" +
            "\"GREATER_THAN_OR_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2," +
            "\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false," +
            "\"value\":52}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"AND\",\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"LESS_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3," +
            "\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false," +
            "\"value\":50.8}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"AND\",\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"AND\",\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1," +
            "\"colVal\":2},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false," +
            "\"value\":4800}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"LESS_THAN_OR_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":" +
            "1,\"colVal\":1},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false," +
            "\"value\":9990}}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":" +
            "4,\"operator\":\"AND\",\"left\":{\"exprType\":\"BINARY\",\"returnType\":" +
            "4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":" +
            "1,\"colVal\":0},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false," +
            "\"value\":1}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":" +
            "4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":" +
            "2,\"colVal\":3},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false," +
            "\"value\":3000000000}}}}}}";
        OmniFilterAndProjectOperatorFactory factoryJSON =
                new OmniFilterAndProjectOperatorFactory(strJSON, types, projectionsJSON, 1);

        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();

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
        IntVec col1_ = new IntVec(numRows);
        IntVec col2_ = new IntVec(numRows);
        IntVec col3_ = new IntVec(numRows);
        LongVec col4_ = new LongVec(numRows);
        DoubleVec col5_ = new DoubleVec(numRows);
        LongVec col6_ = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1_.set(i, i % 3 == 0 ? 0 : 1);
            col2_.set(i, i);
            col3_.set(i, i);
            col4_.set(i, i % 2 == 0 ? 3000000000L : 2999999999L);
            col5_.set(i, 50 + i / 10D);
            col6_.set(i, i % 55);
        }

        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2, col3, col4, col5, col6);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col1_, col2_, col3_, col4_, col5_, col6_);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 543);
        assertEquals(resJSON.getRowCount(), 543);
        IntVec res0 = ((IntVec) res.getVector(0));
        IntVec res1 = ((IntVec) res.getVector(1));
        IntVec resJSON0 = ((IntVec) resJSON.getVector(0));
        IntVec resJSON1 = ((IntVec) resJSON.getVector(1));
        DoubleVec res2 = ((DoubleVec) res.getVector(2));
        LongVec res3 = ((LongVec) res.getVector(3));
        DoubleVec resJSON2 = ((DoubleVec) resJSON.getVector(2));
        LongVec resJSON3 = ((LongVec) resJSON.getVector(3));
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue((res0.get(i) != 1 && res1.get(i) > 4800 && res2.get(i) < 50.8) || res3.get(i) >= 52);
            assertTrue((resJSON0.get(i) != 1 && resJSON1.get(i) > 4800 && resJSON2.get(i) < 50.8) || resJSON3.get(i) >= 52);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Logical operators 2.
     */
    @Test
    public void logicalOperators2() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, LongDataType.LONG, LongDataType.LONG};
        List<String> projections = ImmutableList.of("#3", "#2", "#1", "#0");
        String str = "AND:4(OR:4($operator$LESS_THAN:4(#0, 50:1), $operator$EQUAL:4(#1, -12:1)), "
                + "OR:4($operator$LESS_THAN_OR_EQUAL:4(#2, -3000000000:2), $operator$GREATER_THAN_OR_EQUAL:4(#3, 0:2)))";
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":3}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":2}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projections);
        String strJSON =
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\",\"left\":" +
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\",\"left\":" +
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":50}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":" +
            "\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":-12}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":" +
            "\"OR\",\"left\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":" +
            "\"LESS_THAN_OR_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2," +
            "\"colVal\":2},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false," +
            "\"value\":-3000000000}},\"right\":{\"exprType\":\"BINARY\",\"returnType\":" +
            "4,\"operator\":\"GREATER_THAN_OR_EQUAL\",\"left\":{\"exprType\":" +
            "\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":3},\"right\":{\"exprType\":\"LITERAL\"," +
            "\"dataType\":2,\"isNull\":false,\"value\":0}}}}";
        OmniFilterAndProjectOperatorFactory factoryJSON =
                new OmniFilterAndProjectOperatorFactory(strJSON, types, projectionsJSON, 1);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();

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
        IntVec col1_ = new IntVec(numRows);
        IntVec col2_ = new IntVec(numRows);
        LongVec col3_ = new LongVec(numRows);
        LongVec col4_ = new LongVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1_.set(i, i % 100);
            col2_.set(i, i % 7 == 0 ? -12 : i);
            col3_.set(i, i % 8 == 0 ? -i - 3000000000L : i + 3000000000L);
            col4_.set(i, i % 9 - 4);
        }

        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2, col3, col4);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col1_, col2_, col3_, col4_);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 3498);
        assertEquals(resJSON.getRowCount(), 3498);
        LongVec res0 = ((LongVec) res.getVector(0));
        LongVec res1 = ((LongVec) res.getVector(1));
        IntVec res2 = ((IntVec) res.getVector(2));
        IntVec res3 = ((IntVec) res.getVector(3));
        LongVec resJSON0 = ((LongVec) resJSON.getVector(0));
        LongVec resJSON1 = ((LongVec) resJSON.getVector(1));
        IntVec resJSON2 = ((IntVec) resJSON.getVector(2));
        IntVec resJSON3 = ((IntVec) resJSON.getVector(3));
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue((res0.get(i) >= 0 || res1.get(i) <= -3000000000L) && (res2.get(i) == -12 || res3.get(i) < 50));
            assertTrue((resJSON0.get(i) >= 0 || resJSON1.get(i) <= -3000000000L) && (resJSON2.get(i) == -12 || resJSON3.get(i) < 50));
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Logical operators 3.
     */
    @Test
    public void logicalOperators3() {
        DataType[] types = {IntDataType.INTEGER, DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#1", "#0");
        String expr = "AND:4($operator$NOT_EQUAL:4(#1, 0:3), OR:4(OR:4(OR:4($operator$EQUAL:4(#0, 1:1), "
                + "$operator$EQUAL:4(#0, 2:1)), $operator$EQUAL:4(#0, 3:1)), OR:4(OR:4(OR:4($operator$EQUAL:4(55:1, #0), "
                + "$operator$EQUAL:4(5:1, #0)), $operator$EQUAL:4(#0, 8:1)), $operator$EQUAL:4(#0, 13:1))))";
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":1}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(expr, types,
            projections);
        String exprJSON =
            "{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"AND\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"NOT_EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":1}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":0}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":2}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":3}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"OR\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":55}," +
            "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":5}," +
            "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":8}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":13}}}}}";
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(exprJSON, types,
                projectionsJSON, 1);
        final int numRows = 10000;
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();

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
        IntVec col1_ = new IntVec(numRows);
        DoubleVec col2_ = new DoubleVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1_.set(i, 0);
            col2_.set(i, 1.5);
        }
        col1_.set(0, 0);
        col1_.set(1, 1);
        col1_.set(2, 1);
        col1_.set(3, 2);
        col1_.set(4, 3);
        col1_.set(5, 5);
        col1_.set(6, 8);
        col1_.set(7, 13);
        col2_.set(2, 0);

        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col1_, col2_);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 6);
        int vals[] = {1, 2, 3, 5, 8, 13};
        for (int i = 0; i < res.getRowCount(); i++) {
            assertEquals(((IntVec) res.getVector(1)).get(i), vals[i]);
            assertEquals(((IntVec) resJSON.getVector(1)).get(i), vals[i]);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Arithmetic add.
     */
    @Test
    public void arithmeticAdd() {
        DataType[] types = {IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "$operator$GREATER_THAN:4(ADD:1(#0, 1:1), 4:1)", types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\"," +
            "\"left\":{\"exprType\":\"BINARY\",\"returnType\":1," +
                "\"operator\":\"ADD\"," +
                "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
                "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":4}}",
            types, projectionsJSON, 1);
        final int numRows = 10000;
        IntVec col1 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 5);
        }
        IntVec col1_ = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1_.set(i, i % 5);
        }
        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col1_);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
        assertEquals(resJSON.getRowCount(), 2000);
        for (int i = 0; i < res.getRowCount(); i++) {
            assertTrue(((IntVec) res.getVector(0)).get(i) + 1 > 4);
            assertTrue(((IntVec) resJSON.getVector(0)).get(i) + 1 > 4);
        }

        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
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
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, DoubleDataType.DOUBLE, DoubleDataType.DOUBLE};
        List<String> projections = ImmutableList.of("#0", "#1", "#2", "#3");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":2}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":3}");
        String str = "$operator$LESS_THAN_OR_EQUAL:4(#0, 500:1)";
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(str, types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4," +
            "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
            "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
            "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":500}}",
            types, projectionsJSON, 1);
        final int numRows = 1000;
        for (int i = 0; i < 1000; i++) {
            Thread thread = new Thread(() -> {
                OmniOperator op = factory.createOperator();
                VecBatch vecBatch = new VecBatch(createTable(numRows));
                op.addInput(vecBatch);
                assertTrue(op.getOutput().hasNext());
                VecBatch res = op.getOutput().next();
                // System.out.println(res.getLength());
                assertEquals(res.getRowCount(), 501);

                freeVecBatch(res);
                op.close();
            });
            thread.setName("thread" + i);
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

        // test in JSON format
        for (int i = 0; i < 1000; i++) {
            Thread thread = new Thread(() -> {
                OmniOperator opJSON = factoryJSON.createOperator();
                VecBatch vecBatch = new VecBatch(createTable(numRows));
                opJSON.addInput(vecBatch);
                assertTrue(opJSON.getOutput().hasNext());
                VecBatch resJSON = opJSON.getOutput().next();
                // System.out.println(resJSON.getLength());
                assertEquals(resJSON.getRowCount(), 501);

                freeVecBatch(resJSON);
                opJSON.close();
            });
            thread.setName("thread" + i);
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

        factory.close();
        factoryJSON.close();
    }

    /**
     * Conditional.
     */
    @Test
    public void conditional() {
        DataType[] types = {IntDataType.INTEGER, IntDataType.INTEGER, IntDataType.INTEGER};
        List<String> projections = ImmutableList.of("#0", "#1", "#2");
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}",
                        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":2}");
        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "AND:4(IF:4($operator$EQUAL:4(#0, 0:1), $operator$LESS_THAN:4(#1, 3:1), $operator$EQUAL:4(#1, 4:1)), "
                        + "$operator$GREATER_THAN:4(#2, 3:1))",
                types, projections);
        OmniFilterAndProjectOperatorFactory factoryJSON = new OmniFilterAndProjectOperatorFactory(
            "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
            "\"left\":{\"exprType\":\"IF\",\"returnType\":4," +
            "\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                "\"operator\":\"EQUAL\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":0}}," +
            "\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                "\"operator\":\"LESS_THAN\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":3}}," +
            "\"if_false\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                "\"operator\":\"EQUAL\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":4}}}," +
            "\"right\":{\"exprType\":\"BINARY\",\"returnType\":4," +
                "\"operator\":\"GREATER_THAN\"," +
                    "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":2}," +
                    "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":3}}}",
            types, projectionsJSON, 1);
        final int numRows = 10000;
        IntVec col1 = new IntVec(numRows);
        IntVec col2 = new IntVec(numRows);
        IntVec col3 = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1.set(i, i % 2);
            col2.set(i, i % 5);
            col3.set(i, i % 10);
        }
        IntVec col1_ = new IntVec(numRows);
        IntVec col2_ = new IntVec(numRows);
        IntVec col3_ = new IntVec(numRows);
        for (int i = 0; i < numRows; i++) {
            col1_.set(i, i % 2);
            col2_.set(i, i % 5);
            col3_.set(i, i % 10);
        }

        OmniOperator op = factory.createOperator();
        OmniOperator opJSON = factoryJSON.createOperator();
        ImmutableList<VecBatch> vecBatches1 = makeInput(numRows, col1, col2, col3);
        ImmutableList<VecBatch> vecBatches2 = makeInput(numRows, col1_, col2_, col3_);
        for (VecBatch vecBatch : vecBatches1) {
            op.addInput(vecBatch);
        }
        for (VecBatch vecBatch : vecBatches2) {
            opJSON.addInput(vecBatch);
        }

        assertTrue(op.getOutput().hasNext());
        assertTrue(opJSON.getOutput().hasNext());
        VecBatch res = op.getOutput().next();
        VecBatch resJSON = opJSON.getOutput().next();
        assertEquals(res.getRowCount(), 2000);
        assertEquals(resJSON.getRowCount(), 2000);
        freeVecBatch(res);
        op.close();
        opJSON.close();
        factory.close();
        factoryJSON.close();
    }

    /**
     * Unsupported expression.
     */
    @Test
    public void unsupportedExpr() {
        DataType[] types = {DoubleDataType.DOUBLE};
        List<String> projectionsJSON =
                ImmutableList.of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0}");

        OmniFilterAndProjectOperatorFactory factory = new OmniFilterAndProjectOperatorFactory(
                "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"CAST\"," +
                        "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":3,\"colVal\":0},\"right\"" +
                        ":{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":1.0}}",
                types, projectionsJSON, 1);

        assertFalse(factory.isSupported());
        factory.close();
    }
}
