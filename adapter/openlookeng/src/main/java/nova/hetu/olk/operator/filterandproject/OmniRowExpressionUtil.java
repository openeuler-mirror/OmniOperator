/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.UnknownType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.VariableReferenceExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;

import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.type.CharVecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static nova.hetu.olk.tool.OperatorUtils.toVecType;

/**
 * Util class for openLooKeng RowExpression
 *
 * @since 20210901
 */
public class OmniRowExpressionUtil {
    /**
     * Enum type to choose which format to parse RowExpression into
     */
    public enum Format {
        STRING,
        JSON;
    }

    private OmniRowExpressionUtil() {}

    // Unchecked Exception wrapper
    private static class RowExpressionJsonProcessingException extends RuntimeException {
        RowExpressionJsonProcessingException(Throwable e) {
            super(e);
        };
    }

    /**
     * Wrapper function that selectively stringify RowExpression into string / jsonString
     *
     * @param rowExpression RowExpression from openLooKeng
     * @param format enum var indicates returning String / JSON String
     * @return String / JSON String representation of the RowExpression
     */
    public static String expressionStringify(RowExpression rowExpression, Format format) {
        switch (format) {
            case JSON:
                String expressionJsonString = null;
                try {
                    expressionJsonString = expressionJsonify(rowExpression);
                } catch (JsonProcessingException e) {
                    throw new RowExpressionJsonProcessingException(e);
                }
                return expressionJsonString;
            case STRING:
            default:
                return expressionStringify(rowExpression);
        }
    }

    /**
     * Stringify a RowExpression
     *
     * @param rowExpression RowExpression from openLooKeng
     * @return String representation of the RowExpression
     */
    public static String expressionStringify(RowExpression rowExpression) {
        if (rowExpression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) rowExpression;
            List<String> args = callExpression.getArguments().stream()
                .map(OmniRowExpressionUtil::expressionStringify).collect(Collectors.toList());
            return callExpression.getSignature().getName() + ":" +
                    signatureToVecTypeId(callExpression.getType().getTypeSignature()) +
                    "(" + Joiner.on(", ").join(args) + ")";
        }

        if (rowExpression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) rowExpression;
            List<String> args = specialForm.getArguments().stream()
                .map(OmniRowExpressionUtil::expressionStringify).collect(Collectors.toList());
            return specialForm.getForm().name() + ":" +
                    toVecType(specialForm.getType().getTypeSignature()).getId().ordinal() +
                    "(" + Joiner.on(", ").join(args) + ")";
        }

        if (rowExpression instanceof LambdaDefinitionExpression) {
            LambdaDefinitionExpression lambdaDefinitionExpression = (LambdaDefinitionExpression) rowExpression;
            return "(" + Joiner.on(", ").join(lambdaDefinitionExpression.getArguments()) + ") -> " +
                lambdaDefinitionExpression.getBody();
        }

        if (rowExpression instanceof InputReferenceExpression ||
                rowExpression instanceof VariableReferenceExpression) {
            return rowExpression.toString();
        }

        if (rowExpression instanceof ConstantExpression) {
            ConstantExpression constantExpression = (ConstantExpression) rowExpression;
            Type type = rowExpression.getType();
            if (type instanceof UnknownType && constantExpression.getValue() == null) {
                return "NULL:0";
            }

            if ((type instanceof VarcharType || type instanceof CharType)
                && constantExpression.getValue() instanceof Slice) {
                String varcharValue = ((Slice) constantExpression.getValue()).toStringAscii();
                return "'" + varcharValue + "':" + signatureToVecTypeId(type.getTypeSignature());
            }

            if (type instanceof DecimalType && !((DecimalType) type).isShort()
                && constantExpression.getValue() instanceof Slice) {
                return Decimals.decodeUnscaledValue((Slice) constantExpression.getValue()) + ":" +
                    signatureToVecTypeId(type.getTypeSignature());
            }

            return constantExpression.getValue() + ":" + signatureToVecTypeId(type.getTypeSignature());
        }
        return rowExpression.toString();
    }

    /**
     * Get VecTypeId with width from the type signature
     *
     * @param signature Signature from openLooKeng
     * @return VecTypeId corresponding to Type
     */
    public static String signatureToVecTypeId(TypeSignature signature) {
        if ("char".equalsIgnoreCase(signature.getBase())) {
            int width = signature.getParameters().get(0).getLongLiteral().intValue();
            return String.format("%d[%d]", toVecType(signature).getId().ordinal(), width);
        }
        return String.valueOf(toVecType(signature).getId().ordinal());
    }

    /**
     * Jsonify a RowExpression into JSON string
     *
     * @param rowExpression RowExpression from openLooKeng
     * @return String representation of the RowExpression in JSON structure
     * @throws JsonProcessingException when error converting JSON to String
     */
    public static String expressionJsonify(RowExpression rowExpression)
            throws JsonProcessingException {
        ObjectNode jsonRoot = rowExpression.accept( new JsonifyVisitor(), null);
        return new ObjectMapper().writeValueAsString(jsonRoot);
    }
}

class JsonifyVisitor implements RowExpressionVisitor<ObjectNode, Void> {
    private static final String OPERATOR_PREFIX = "$operator$";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> ARITH_BIN_OPS = new ArrayList<>(Arrays.asList("ADD", "SUBTRACT", "MULTIPLY",
            "DIVIDE", "MODULUS"));
    private static final  List<String> COM_BIN_OPS = new ArrayList<>(Arrays.asList("GREATER_THAN",
            "GREATER_THAN_OR_EQUAL", "LESS_THAN", "LESS_THAN_OR_EQUAL", "EQUAL", "NOT_EQUAL"));
    private static final List<String> UNARY_OPS = new ArrayList<>(Arrays.asList("NEGATION","NOT"));

    @Override
    public ObjectNode visitCall(CallExpression call, Void context) {
        ObjectNode callRoot = MAPPER.createObjectNode();
        // demangle name to get rid of OPERATOR_PREFIX
        String callName = call.getSignature().getName();
        if (callName.startsWith(OPERATOR_PREFIX)) {
            callName = callName.substring(OPERATOR_PREFIX.length());
        }
        TypeSignature callSignature = call.getType().getTypeSignature();
        int returnType = OperatorUtils.toVecType(callSignature).getId().ordinal();
        // Binary operator in rowExpression
        if (ARITH_BIN_OPS.contains(callName) || COM_BIN_OPS.contains(callName)) {
            callRoot.put("exprType", "BINARY")
                    .put("returnType", returnType)
                    .put("operator", callName)
                    .set("left", call.getArguments().get(0).accept(this, context));
            callRoot.set("right", call.getArguments().get(1).accept(this, context));
        } else if (UNARY_OPS.contains(callName)) {
            // Unary operator in rowExpression
            callRoot.put("exprType", "UNARY")
                    .put("returnType", returnType)
                    .put("operator", callName)
                    .set("expr", call.getArguments().get(0).accept(this, context));
        } else {
            // Function call in rowExpression
            ArrayNode arguments = MAPPER.createArrayNode();
            // Process all arguments of this function call
            for (RowExpression argument : call.getArguments()) {
                arguments.add(argument.accept(this, context));
            }
            callRoot.put("exprType", "FUNCTION")
                    .put("returnType", returnType)
                    .put("function_name", callName)
                    .set("arguments", arguments);
            if ("char".equalsIgnoreCase(callSignature.getBase())) {
                callRoot.put("width",
                        callSignature.getParameters().get(0).getLongLiteral().intValue());
            }
        }
        return callRoot;
    }

    @Override
    public ObjectNode visitSpecialForm(SpecialForm specialForm, Void context) {
        ObjectNode specialFormRoot = MAPPER.createObjectNode();
        String formName = specialForm.getForm().name();
        int returnType = OperatorUtils.toVecType(specialForm.getType().getTypeSignature()).getId().ordinal();
        switch (formName) {
            case "AND":
            case "OR":
                specialFormRoot.put("exprType", "BINARY")
                        .put("returnType", returnType)
                        .put("operator", formName)
                        .set("left", specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("right", specialForm.getArguments().get(1).accept(this, context));
                break;
            case "BETWEEN":
                specialFormRoot.put("exprType", "BETWEEN")
                        .put("returnType", returnType)
                        .set("value", specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("lower_bound",
                        specialForm.getArguments().get(1).accept(this, context));
                specialFormRoot.set("upper_bound",
                        specialForm.getArguments().get(2).accept(this, context));
                break;
            case "IF":
                specialFormRoot.put("exprType", "IF")
                        .put("returnType", returnType)
                        .set("condition", specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("if_true", specialForm.getArguments().get(1).accept(this, context));
                specialFormRoot.set("if_false", specialForm.getArguments().get(2).accept(this, context));
                break;
            case "COALESCE":
                specialFormRoot.put("exprType", "COALESCE")
                        .put("returnType", returnType)
                        .set("value1", specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("value2", specialForm.getArguments().get(1).accept(this, context));
                break;
            default:
                ArrayNode arguments = MAPPER.createArrayNode();
                // Process all arguments of this function call
                for (RowExpression argument : specialForm.getArguments()) {
                    arguments.add(argument.accept(this, context));
                }
                specialFormRoot.put("exprType", formName)
                        .put("returnType", returnType)
                        .set("arguments", arguments);
                break;
        }
        return specialFormRoot;
    }

    @Override
    public ObjectNode visitInputReference(InputReferenceExpression reference, Void context) {
        ObjectNode inputRefRoot = MAPPER.createObjectNode();
        VecType vecType = OperatorUtils.toVecType(reference.getType().getTypeSignature());
        inputRefRoot.put("exprType", "FIELD_REFERENCE")
                .put("dataType", vecType.getId().ordinal())
                .put("colVal", reference.getField());
        if (vecType instanceof CharVecType) {
            inputRefRoot.put("width", ((CharVecType)vecType).getWidth());
        } else if (vecType instanceof VarcharVecType) {
            inputRefRoot.put("width", ((VarcharVecType)vecType).getWidth());
        }

        return inputRefRoot;
    }

    @Override
    public ObjectNode visitConstant(ConstantExpression literal, Void context) {
        ObjectNode constantRoot = MAPPER.createObjectNode();
        VecType literalType = OperatorUtils.toVecType(literal.getType().getTypeSignature());
        constantRoot.put("exprType", "LITERAL").put("dataType", literalType.getId().ordinal());
        // Null check on expression value
        if (literal.getValue() == null) {
            constantRoot.put("isNull", true);
            return constantRoot;
        }
        constantRoot.put("isNull", false);
        switch (literalType.getId()) {
            case OMNI_VEC_TYPE_BOOLEAN:
                constantRoot.put("value", Boolean.valueOf(literal.getValue().toString()));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                constantRoot.put("value", Double.parseDouble(literal.getValue().toString()));
                break;
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                constantRoot.put("value", Integer.parseInt(literal.getValue().toString()));
                break;
            case OMNI_VEC_TYPE_LONG:
                constantRoot.put("value", Long.parseLong(literal.getValue().toString()));
                break;
            case OMNI_VEC_TYPE_DECIMAL64:
                constantRoot.put("value", (Long.parseLong(literal.getValue().toString())));
                constantRoot.put("precision", ((Decimal64VecType)literalType).getPrecision());
                constantRoot.put("scale", ((Decimal64VecType)literalType).getScale());
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                // FIXME: Need to Support 128 bits properly
                long d128Val;
                if (literal.getValue() instanceof Slice) {
                    d128Val = Decimals.decodeUnscaledValue((Slice) literal.getValue()).longValue();
                } else {
                    d128Val = Long.parseLong(literal.getValue().toString());
                }
                constantRoot.put("value", d128Val);
                constantRoot.put("precision", ((Decimal128VecType)literalType).getPrecision());
                constantRoot.put("scale", ((Decimal128VecType)literalType).getScale());
                break;
            case OMNI_VEC_TYPE_CHAR:
            case OMNI_VEC_TYPE_VARCHAR:
                String varcharValue;
                if (literal.getValue() instanceof Slice) {
                    varcharValue = ((Slice) literal.getValue()).toStringAscii();
                } else {
                    varcharValue = String.valueOf(literal.getValue());
                }
                constantRoot.put("value", varcharValue);
                constantRoot.put("width", ((VarcharVecType) literalType).getWidth());
                break;
            case OMNI_VEC_TYPE_NONE:
                // TODO: Support UNKNOWN presto type in VecType
                // omni-runtime treat NONE regardless of its value
                constantRoot.put("value", "UNKNOWN");
                break;
            default:
                constantRoot.put("invalidVal", "invalidVal");
                break;
        }
        return constantRoot;
    }

    @Override
    public ObjectNode visitLambda(LambdaDefinitionExpression lambda, Void context) {
        ObjectNode lambdaRoot = MAPPER.createObjectNode();
        // TODO: add lambda support in omni-runtime
        lambdaRoot.put("exprType", "LAMBDA");
        return lambdaRoot;
    }

    @Override
    public ObjectNode visitVariableReference(VariableReferenceExpression reference, Void context) {
        ObjectNode varRefRoot = MAPPER.createObjectNode();
        VecType vecType = OperatorUtils.toVecType(reference.getType().getTypeSignature());
        varRefRoot.put("exprType", "VARIABLE_REFERENCE")
                .put("dataType", OperatorUtils.toVecType(reference.getType().getTypeSignature())
                        .getId().ordinal())
                .put("varName", reference.getName());
        if (vecType instanceof CharVecType) {
            varRefRoot.put("width", ((CharVecType)vecType).getWidth());
        } else if (vecType instanceof VarcharVecType) {
            varRefRoot.put("width", ((VarcharVecType)vecType).getWidth());
        }
        return varRefRoot;
    }
}