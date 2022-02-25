package nova.hetu.olk.operator;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.sql.relational.*;
import nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.UnknownType.UNKNOWN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.*;

/**
 * The OmniExpressionUtil tests
 */
@Test(singleThreaded = true)
public class TestOmniExpressionUtil {
    private final InputReferenceExpression SHIP_DATE = field(0, BIGINT);
    private final ConstantExpression CONDITION1 = constant(10000, BIGINT);
    private final ConstantExpression CONDITION2 = constant(10471, BIGINT);
    private final InputReferenceExpression EXTENDED_PRICE = field(1, BIGINT);
    private final InputReferenceExpression EXTENDED_DECIMAL_PRICE = field(1, createDecimalType());
    private final InputReferenceExpression DISCOUNT = field(2, BIGINT);
    private final ConstantExpression BOOL1 = constant(true, BOOLEAN);
    private final ConstantExpression BOOL2 = constant(false, BOOLEAN);

    private final List<OperatorType> CmpOps = Arrays.asList(OperatorType.LESS_THAN_OR_EQUAL,
            OperatorType.LESS_THAN, OperatorType.GREATER_THAN, OperatorType.GREATER_THAN_OR_EQUAL,
            OperatorType.EQUAL, OperatorType.NOT_EQUAL);
    private final List<OperatorType> ArithOps = Arrays.asList(OperatorType.ADD, OperatorType.SUBTRACT,
            OperatorType.MULTIPLY, OperatorType.DIVIDE, OperatorType.MODULUS);
    private final OperatorType UnaryOp = OperatorType.NEGATION;

    @DataProvider(name = "binaryComparisonExpression")
    private Object[][] prepareBinCmpTests() {
        Object[][] testCase = new Object[CmpOps.size()][];
        for( int i = 0; i < CmpOps.size(); i++ ) {
            testCase[i] = new Object[]{
                    call(internalOperator(CmpOps.get(i), BOOLEAN.getTypeSignature(), INTEGER.getTypeSignature(),
                            INTEGER.getTypeSignature()), BOOLEAN, SHIP_DATE, CONDITION1),
                    "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"" +
                            CmpOps.get(i) + "\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":" +
                            "2,\"colVal\":0},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false," +
                            "\"value\":10000}}"};
        }
        return testCase;
    }

    @DataProvider(name = "binaryArithmeticExpression")
    private Object[][] prepareBinArithTests() {
        Object[][] testCase = new Object[ArithOps.size()][];
        for( int i = 0; i < ArithOps.size(); i++ ) {
            testCase[i] = new Object[]{
                    call(internalOperator(ArithOps.get(i), BIGINT.getTypeSignature(), BIGINT.getTypeSignature(),
                        BIGINT.getTypeSignature()), BIGINT, EXTENDED_PRICE, DISCOUNT),
                    "{\"exprType\":\"BINARY\",\"returnType\":2,\"operator\":\"" +
                            ArithOps.get(i) + "\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":" +
                            "2,\"colVal\":1},\"right\":{\"exprType\":\"FIELD_REFERENCE\"," +
                            "\"dataType\":2,\"colVal\":2}}"};
        }
        return testCase;
    }

    @DataProvider(name = "constantExpression")
    private Object[][] prepareConstantExpTests() {
        ByteBuffer stringBuffer = ByteBuffer.wrap("stringVal".getBytes());
        Slice varcharSlice = wrappedBuffer(stringBuffer);

        return new Object[][]{
                {constant(true, BOOLEAN), "{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":true}"},
                {constant(12345678910L, BIGINT), "{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":12345678910}"},
                {constant(1, INTEGER), "{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}"},
                {constant(2.0, DOUBLE), "{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":2.0}"},
                {constant(4, createDecimalType(19)), "{\"exprType\":\"LITERAL\",\"dataType\":7,\"isNull\":false,\"value\":\"4\",\"precision\":19,\"scale\":0}"},
                {constant(5, createDecimalType(37)), "{\"exprType\":\"LITERAL\",\"dataType\":7,\"isNull\":false,\"value\":\"5\",\"precision\":37,\"scale\":0}"},
                {constant(varcharSlice, VARCHAR), "{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"stringVal\",\"width\":1048576}"},
                // Need support of UNKNOWN presto type in VecType
                // {constant("UNKNOWN", UNKNOWN), "{\"exprType\":\"LITERAL\",\"dataType\":0,"isNull":false,\"value\":UNKNOWN}"},
                {constant(null, BIGINT), "{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":true}"}
        };
    }

    /**
     * Test ConstantExpression
     * @param literal ConstantExpression
     * @param expected String
     */
    @Test(dataProvider = "constantExpression")
    public void testConstantExpression(ConstantExpression literal, String expected) {
        String parseRes = OmniRowExpressionUtil.expressionStringify(literal, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, expected,
                literal.getType().getDisplayName() + " parsed result doesn't match");
    }

    /**
     * Test InputReferenceExpression
     */
    @Test
    public void testInputReferenceExpression(){
        String referenceExpected = "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":1}";
        String parseRes = OmniRowExpressionUtil.expressionStringify(EXTENDED_PRICE, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, referenceExpected, "InputReference parsed result doesn't match");
    }

    /**
     * Test DecimalInputReferenceExpression
     */
    @Test
    public void testDecimnalInputReferenceExpression(){
        String referenceExpected = "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":7,\"colVal\":1," +
                "\"precision\":38,\"scale\":0}";
        String parseRes = OmniRowExpressionUtil.expressionStringify(EXTENDED_DECIMAL_PRICE,
                OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, referenceExpected, "InputReference parsed result doesn't match");
    }

    /**
     * Test VariableReferenceExpression
     */
    @Test
    public void testVariableReferenceExpression() {
        VariableReferenceExpression variableReference = new VariableReferenceExpression("testVariable", INTEGER);
        String variableRefExpected = "{\"exprType\":\"VARIABLE_REFERENCE\",\"dataType\":1,\"varName\":\"testVariable\"}";
        String parseRes = OmniRowExpressionUtil.expressionStringify(variableReference, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, variableRefExpected, "variableReference parsed result doesn't match");
    }

    /**
     * Test binary comparison CallExpression
     * @param call CallExpression
     * @param expected String
     */
    @Test(dataProvider = "binaryComparisonExpression")
    public void testCmpBinOps(CallExpression call, String expected) {
        String parseRes = OmniRowExpressionUtil.expressionStringify(call, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, expected, call.getSignature().getName() + " parsed result doesn't match");
    }

    /**
     * Test binary arithmetic CallExpression
     * @param call CallExpression
     * @param expected String
     */
    @Test(dataProvider = "binaryArithmeticExpression")
    public void testArithBinOps(CallExpression call, String expected ) {
        String parseRes = OmniRowExpressionUtil.expressionStringify(call, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, expected, call.getSignature().getName()+ " parsed result doesn't match");
    }

    /**
     * Test unary CallExpression
     */
    @Test
    public void testUnaryOps() {
        CallExpression testNegation = call(
                internalOperator(UnaryOp, BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature()),
                BOOLEAN,
                constant(false, BOOLEAN));
        String unaryOpExpected = "{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"NEGATION\",\"expr\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":false}}";
        String parseRes = OmniRowExpressionUtil.expressionStringify(testNegation, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, unaryOpExpected, "NEGATION parsed result doesn't match");
    }

    @DataProvider(name = "specialForm")
    private Object[][] specialForm() {
        return new Object[][] {
                {new SpecialForm(SpecialForm.Form.AND, BOOLEAN, BOOL1, BOOL2),
                        "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\",\"left\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":true},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":false}}"},
                {new SpecialForm(SpecialForm.Form.OR, BOOLEAN, BOOL1, BOOL2),
                        "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\",\"left\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":true},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":false}}"},
                {new SpecialForm(SpecialForm.Form.BETWEEN, BOOLEAN, SHIP_DATE, CONDITION1, CONDITION2),
                        "{\"exprType\":\"BETWEEN\",\"returnType\":4,\"value\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":2,\"colVal\":0},\"lower_bound\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10000},\"upper_bound\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10471}}"},
                {new SpecialForm(SpecialForm.Form.IF, BIGINT, BOOL1, CONDITION1, CONDITION2),
                        "{\"exprType\":\"IF\",\"returnType\":2,\"condition\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":false,\"value\":true},\"if_true\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10000},\"if_false\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10471}}"},
                {new SpecialForm(SpecialForm.Form.COALESCE, BIGINT, CONDITION1, CONDITION2),
                        "{\"exprType\":\"COALESCE\",\"returnType\":2,\"value1\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10000},\"value2\":{\"exprType\":\"LITERAL\",\"dataType\":2,\"isNull\":false,\"value\":10471}}"}
        };
    }

    /**
     * Test SpecialForm
     * @param specialForm SpecialForm
     * @param expected String
     */
    @Test(dataProvider = "specialForm")
    public void testSpecialForm(SpecialForm specialForm, String expected) {
        String parseRes = OmniRowExpressionUtil.expressionStringify(specialForm, OmniRowExpressionUtil.Format.JSON);
        Assert.assertEquals(parseRes, expected, specialForm.getForm().toString()+ " parsed result doesn't match");
    }

    /**
     * Test Lambda Expression
     */
    @Test
    public void testLambdaDefinitionExpression() {
        // TODO: Add tests when implemented
    }

}
