/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

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
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.VariableReferenceExpression;

import java.util.List;
import java.util.stream.Collectors;

import static nova.hetu.olk.tool.OperatorUtils.toVecType;

/**
 * Util class for openLooKeng RowExpression
 *
 * @since 20210901
 */
public class OmniRowExpressionUtil {
    private OmniRowExpressionUtil() {}

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
}
