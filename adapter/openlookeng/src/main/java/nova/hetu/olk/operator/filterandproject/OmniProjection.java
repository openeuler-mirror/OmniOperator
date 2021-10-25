/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;

import java.util.List;
import java.util.Optional;

/**
 * The type Omni projection.
 *
 * @since 20210630
 */
public class OmniProjection {
    private final OmniProjectOperatorFactory omniProjectionFactory;

    private final int projectLength;

    /**
     * Instantiates a new Omni projection.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     * @param filter the filter
     */
    public OmniProjection(List<? extends RowExpression> expressions, List<Type> inputTypes,
        Optional<RowExpression> filter) {
        this.projectLength = expressions.size();
        this.omniProjectionFactory = new OmniProjectOperatorFactory(
            expressions.stream().map(OmniRowExpressionUtil::expressionStringify)
            .toArray(String[]::new), toVecTypes(inputTypes));
    }

    /**
     * Gets factory.
     *
     * @return the factory
     */
    public OmniProjectOperatorFactory getFactory() {
        return this.omniProjectionFactory;
    }

    /**
     * Is empty boolean.
     *
     * @return the boolean
     */
    public boolean isEmpty() {
        return this.projectLength == 0;
    }

    /**
     * Size int.
     *
     * @return the int
     */
    public int size() {
        return this.projectLength;
    }
}
