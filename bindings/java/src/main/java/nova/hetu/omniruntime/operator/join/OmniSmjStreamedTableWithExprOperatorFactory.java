/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
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
     * @param isJitEnabled whether the jit is enabled
     */
    public OmniSmjStreamedTableWithExprOperatorFactory(DataType[] sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, JoinType joinType, Optional<String> filter, boolean isJitEnabled) {
        super(new FactoryContext(new JitContext(sourceTypes, equalKeyExprs, outputChannels, joinType, filter),
                isJitEnabled));
    }

    /**
     * Instantiates a new Omni sort merge streamed table factory with jit
     * default.
     *
     * @param sourceTypes the all input vector types
     * @param equalKeyExprs equal condition key expressions
     * @param outputChannels output of streamed table
     * @param joinType join type
     * @param filter condition for not equal expression
     */
    public OmniSmjStreamedTableWithExprOperatorFactory(DataType[] sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, JoinType joinType, Optional<String> filter) {
        this(sourceTypes, equalKeyExprs, outputChannels, joinType, filter, true);
    }

    private static native long createSmjStreamedTableWithExprOperatorFactory(String sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, int joinType, String filter, long jitContext);

    private static native long createSmjStreamedTableWithExprJitContext(String sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, int joinType, String filter);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        String filter = context.filter.isPresent() ? context.filter.get() : null;
        return createSmjStreamedTableWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.equalKeyExprs, context.outputChannels, context.joinType.getValue(), filter,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 2021-10-30
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final String[] equalKeyExprs;

        private final int[] outputChannels;

        private final JoinType joinType;

        private final Optional<String> filter;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the all input vector types
         * @param equalKeyExprs equal condition key expressions
         * @param outputChannels output of streamed table
         * @param joinType join type
         * @param filter condition for not equal expression
         */
        public JitContext(DataType[] sourceTypes, String[] equalKeyExprs, int[] outputChannels, JoinType joinType,
                Optional<String> filter) {
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes");
            this.equalKeyExprs = requireNonNull(equalKeyExprs, "equalKeyExprs");
            this.outputChannels = requireNonNull(outputChannels, "outputChannels");
            this.joinType = requireNonNull(joinType, "joinType");
            this.filter = requireNonNull(filter, "filter");
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(equalKeyExprs),
                    Arrays.hashCode(outputChannels), joinType, filter);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext that = (JitContext) obj;
            return Arrays.equals(sourceTypes, that.sourceTypes) && Arrays.equals(equalKeyExprs, that.equalKeyExprs)
                    && Arrays.equals(outputChannels, that.outputChannels) && joinType == that.joinType
                    && filter.equals(that.filter);
        }
    }

    /**
     * The type Factory context.
     *
     * @since 2021-10-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         * @param isJitEnabled whether the jit is enabled
         */
        public FactoryContext(JitContext jitContext, boolean isJitEnabled) {
            super(jitContext, isJitEnabled);
            setNeedCache(false);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            String filter = context.filter.isPresent() ? context.filter.get() : null;
            return createSmjStreamedTableWithExprJitContext(DataTypeSerializer.serialize(context.sourceTypes),
                    context.equalKeyExprs, context.outputChannels, context.joinType.getValue(), filter);
        }
    }
}
