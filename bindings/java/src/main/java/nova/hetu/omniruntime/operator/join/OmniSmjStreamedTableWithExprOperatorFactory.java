/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * The type Omni sort merge streamed table with expression operator factory.
 *
 * @since 20211030
 */
public class OmniSmjStreamedTableWithExprOperatorFactory
        extends
            OmniOperatorFactory<OmniSmjStreamedTableWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni sort merge streamed table factory.
     *
     * @param sourceTypes the all input vector types
     * @param equalKeyExprs  equal condition key expressions
     * @param outputChannels output of streamed table
     * @param joinType join type
     * @param filter condition for not equal expression
     */
    public OmniSmjStreamedTableWithExprOperatorFactory(VecType[] sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, JoinType joinType, Optional<String> filter) {
        super(new FactoryContext(new JitContext(sourceTypes, equalKeyExprs, outputChannels, joinType, filter)));
    }

    private static native long createSmjStreamedTableWithExprOperatorFactory(String sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, int joinType, String filter, long jitContext);

    private static native long createSmjStreamedTableWithExprJitContext(String sourceTypes, String[] equalKeyExprs,
            int[] outputChannels, int joinType, String filter);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        String filter = context.filter.isPresent() ? context.filter.get() : null;
        return createSmjStreamedTableWithExprOperatorFactory(VecTypeSerializer.serialize(context.sourceTypes),
                context.equalKeyExprs, context.outputChannels, context.joinType.getValue(), filter,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20211030
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] sourceTypes;

        private final String[] equalKeyExprs;

        private final int[] outputChannels;

        private final JoinType joinType;

        private final Optional<String> filter;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the all input vector types
         * @param equalKeyExprs  equal condition key expressions
         * @param outputChannels  output of streamed table
         * @param joinType join type
         * @param filter  condition for not equal expression
         */
        public JitContext(VecType[] sourceTypes, String[] equalKeyExprs, int[] outputChannels, JoinType joinType,
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
     * @since 20211030
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         */
        public FactoryContext(JitContext jitContext) {
            super(jitContext);
            setNeedCache(false);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            String filter = context.filter.isPresent() ? context.filter.get() : null;
            return createSmjStreamedTableWithExprJitContext(VecTypeSerializer.serialize(context.sourceTypes),
                    context.equalKeyExprs, context.outputChannels, context.joinType.getValue(), filter);
        }
    }
}
