/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.expressionStringify;
import static nova.hetu.olk.tool.OperatorUtils.toDataTypes;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;

import java.util.List;

/**
 * The type Omni projection.
 *
 * @since 20210630
 */
public class OmniProjection {
    private final OmniProjectOperatorFactory omniProjectionFactory;

    private final int projectLength;
    private final boolean isSupported;

    /**
     * Instantiates a new Omni projection.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     */
    public OmniProjection(List<? extends RowExpression> expressions, List<Type> inputTypes,
            OmniRowExpressionUtil.Format parseFormat) {
        this.projectLength = expressions.size();
        this.omniProjectionFactory = new OmniProjectOperatorFactory(
                expressions.stream().map(p -> expressionStringify(p, parseFormat)).toArray(String[]::new),
                toDataTypes(inputTypes), parseFormat.ordinal());
        this.isSupported = omniProjectionFactory.isSupported();
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

    /**
     * Check if the projection is supported by OmniRuntime
     *
     * @return if the projection is supported
     */
    public boolean isSupported() {
        return isSupported;
    }
}
