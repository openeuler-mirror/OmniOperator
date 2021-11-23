/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.expressionStringify;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;

import io.prestosql.operator.project.InputChannels;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.SelectedPositions;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Omni page filter.
 *
 * @since 20210630
 */
public class OmniPageFilter implements PageFilter {
    private final InputChannels inputChannels;

    private OmniFilterAndProjectOperatorFactory operatorFactory;

    private final boolean isDeterministic;

    private boolean isSupported;

    private final List<Type> inputTypes;

    private final List<? extends RowExpression> projects;

    /**
     * Instantiates a new Omni page filter.
     *
     * @param rowExpression   the row expression
     * @param isDeterministic the is deterministic
     * @param inputChannels   the input channels
     * @param inputTypes      the input types
     * @param projects        the projects
     */
    public OmniPageFilter(RowExpression rowExpression, boolean isDeterministic, InputChannels inputChannels,
                          List<Type> inputTypes, List<? extends RowExpression> projects) {
        RowExpression filterExpression = requireNonNull(rowExpression, "filterExpression is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.isDeterministic = isDeterministic;
        this.projects = projects;
        this.inputTypes = inputTypes;

        VecType[] vecTypes = toVecTypes(inputTypes);
        try {
            this.operatorFactory = new OmniFilterAndProjectOperatorFactory(expressionStringify(filterExpression),
                vecTypes,
                projects.stream().map(OmniRowExpressionUtil::expressionStringify).collect(Collectors.toList()));
            this.isSupported = this.operatorFactory.isSupported();
        } catch (OmniRuntimeException e) {
            isSupported = false;
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
     * @param vecAllocator vector allocator
     * @return the operator
     */
    public OmniPageFilterOperator getOperator(VecAllocator vecAllocator) {
        return new OmniPageFilterOperator(operatorFactory.createOperator(vecAllocator), inputChannels, inputTypes,
            projects);
    }

    /**
     * Is expression supported boolean.
     *
     * @return if the filter is supported by OmniRuntime
     */
    public boolean isSupported() {
        return isSupported;
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

        private InputChannels inputChannels;

        private final List<Type> inputTypes;

        private final List<? extends RowExpression> projects;

        /**
         * Instantiates a new Omni page filter operator.
         *
         * @param operator      the operator
         * @param inputChannels the input channels of filter
         * @param inputTypes    the input types
         * @param projects      the projects
         */
        public OmniPageFilterOperator(OmniOperator operator, InputChannels inputChannels, List<Type> inputTypes,
                                      List<? extends RowExpression> projects) {
            this.operator = operator;
            this.inputChannels = inputChannels;
            this.inputTypes = inputTypes;
            this.projects = projects;
        }

        /**
         * Filter with project page.
         *
         * @param vecBatch the page
         * @return the page
         */
        public VecBatch filterAndProject(VecBatch vecBatch) {
            if (vecBatch.getRowCount() <= 0) {
                return null;
            }
            operator.addInput(vecBatch);
            Iterator<VecBatch> result = operator.getOutput();

            if (!result.hasNext()) {
                return null;
            }
            return result.next();
        }

        /**
         * Close.
         */
        public void close() {
            operator.close();
        }

        /**
         * Get input channels.
         *
         * @return Return input channels.
         */
        public InputChannels getInputChannels() {
            return inputChannels;
        }
    }
}
