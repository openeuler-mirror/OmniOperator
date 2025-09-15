/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The type Omni sort merge streamed table with expression operator factory.
 *
 * @since 2021-10-30
 */
public class OmniSmjStreamedTableWithExprOperatorFactory
        extends OmniOperatorFactory<OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni sort merge streamed table factory.
     *
     * @param sourceTypes the all input vector types
     * @param equalKeyExprs equal condition key expressions
     * @param outputChannels output of streamed table
     * @param joinType join type
     * @param filter condition for not equal expression
     * @param operatorConfig the operator config
     */
    public OmniSmjStreamedTableWithExprOperatorFactory(DataType[] sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, JoinType joinType, Optional<String> filter, OperatorConfig operatorConfig) {
        super(new FactoryContext(sourceTypes, equalKeyExprs, outputChannels, joinType, filter, operatorConfig));
    }

    /**
     * Instantiates a new Omni sort merge streamed table factory with default
     * operator config.
     *
     * @param sourceTypes the all input vector types
     * @param equalKeyExprs equal condition key expressions
     * @param outputChannels output of streamed table
     * @param joinType join type
     * @param filter condition for not equal expression
     */
    public OmniSmjStreamedTableWithExprOperatorFactory(DataType[] sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, JoinType joinType, Optional<String> filter) {
        this(sourceTypes, equalKeyExprs, outputChannels, joinType, filter, new OperatorConfig());
    }

    private static native long createSmjStreamedTableWithExprOperatorFactory(String sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, int joinType, String filter, String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        String filter = context.filter.isPresent() ? context.filter.get() : null;
        return createSmjStreamedTableWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.equalKeyExprs, context.outputChannels, context.joinType.getValue(), filter,
                OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * The type Factory context.
     *
     * @since 2021-10-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final DataType[] sourceTypes;

        private final String[] equalKeyExprs;

        private final int[] outputChannels;

        private final JoinType joinType;

        private final Optional<String> filter;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the all input vector types
         * @param equalKeyExprs equal condition key expressions
         * @param outputChannels output of streamed table
         * @param joinType join type
         * @param filter condition for not equal expression
         * @param operatorConfig the operator config
         */
        public FactoryContext(DataType[] sourceTypes, String[] equalKeyExprs, int[] outputChannels, JoinType joinType,
                Optional<String> filter, OperatorConfig operatorConfig) {
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes");
            this.equalKeyExprs = requireNonNull(equalKeyExprs, "equalKeyExprs");
            this.outputChannels = requireNonNull(outputChannels, "outputChannels");
            this.joinType = requireNonNull(joinType, "joinType");
            this.filter = requireNonNull(filter, "filter");
            this.operatorConfig = operatorConfig;
            setNeedCache(false);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(equalKeyExprs),
                    Arrays.hashCode(outputChannels), joinType, filter, operatorConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FactoryContext that = (FactoryContext) obj;
            return Arrays.equals(sourceTypes, that.sourceTypes) && Arrays.equals(equalKeyExprs, that.equalKeyExprs)
                    && Arrays.equals(outputChannels, that.outputChannels) && joinType == that.joinType
                    && filter.equals(that.filter) && operatorConfig.equals(that.operatorConfig);
        }
    }
}
