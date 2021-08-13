/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.getVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;

import io.prestosql.operator.project.InputChannels;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.SelectedPositions;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;

/**
 * The type Omni page filter.
 *
 * @since 20210630
 */
public class OmniPageFilter implements PageFilter {
    private final InputChannels inputChannels;

    private OmniFilterAndProjectOperatorFactory operatorFactory;

    private final boolean isDeterministic;

    private boolean isExpressionSupported;

    private final List<Type> inputTypes;

    private final int[] projects;

    /**
     * Instantiates a new Omni page filter.
     *
     * @param rowExpression the row expression
     * @param isDeterministic the is deterministic
     * @param inputChannels the input channels
     * @param inputTypes the input types
     * @param projects the projects
     */
    public OmniPageFilter(RowExpression rowExpression, boolean isDeterministic, InputChannels inputChannels,
        List<Type> inputTypes, int[] projects) {
        RowExpression filterExpression = requireNonNull(rowExpression, "filterExpression is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.isDeterministic = isDeterministic;
        this.projects = projects;
        this.inputTypes = inputTypes;

        VecType[] vecTypes = toVecTypes(inputTypes);
        try {
            this.operatorFactory = new OmniFilterAndProjectOperatorFactory(filterExpression.toString(), vecTypes,
                projects);
            this.isExpressionSupported = true;
        } catch (OmniRuntimeException e) {
            isExpressionSupported = false;
        }
    }

    @Override
    public boolean isDeterministic() {
        // not use DictoryAwarePageFilter
        return false;
    }

    @Override
    public InputChannels getInputChannels() {
        return this.inputChannels;
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page) {
        throw new UnsupportedOperationException("OmniPageFilter doesn't support filter without projection");
    }

    /**
     * Gets operator.
     *
     * @return the operator
     */
    public OmniPageFilterOperator getOperator() {
        return new OmniPageFilterOperator(operatorFactory.createOperator(), inputTypes, projects);
    }

    /**
     * Is expression supported boolean.
     *
     * @return the boolean
     */
    public boolean isExpressionSupported() {
        return isExpressionSupported;
    }

    /**
     * Close.
     */
    public void close() {
        // ((JFilterAndProjectOperator) omniOperator).close();
    }

    /**
     * The type Omni page filter operator.
     *
     * @since 20210630
     */
    public static class OmniPageFilterOperator {
        private final OmniOperator operator;

        private final List<Type> inputTypes;

        private final int[] projects;

        /**
         * Instantiates a new Omni page filter operator.
         *
         * @param operator the operator
         * @param inputTypes the input types
         * @param projects the projects
         */
        public OmniPageFilterOperator(OmniOperator operator, List<Type> inputTypes, int[] projects) {
            this.operator = operator;
            this.inputTypes = inputTypes;
            this.projects = projects;
        }

        /**
         * Filter with project page.
         *
         * @param session the session
         * @param page the page
         * @return the page
         */
        public Page filterWithProject(ConnectorSession session, Page page) {
            if (page.getPositionCount() <= 0) {
                page.close();
                return null;
            }
            VecBatch vecBatch = getVecBatch(page, getClass().getSimpleName());
            operator.addInput(vecBatch);
            Iterator<Page> result = new VecBatchToPageIterator(operator.getOutput());

            if (!result.hasNext()) {
                page.close();
                return null;
            }
            Page newPage = result.next();
            page.close();
            return newPage;
        }

        /**
         * Close.
         */
        public void close() {
            operator.close();
        }
    }
}
