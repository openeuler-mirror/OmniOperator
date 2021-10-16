/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The Omni lookup join with expression operator factory.
 */
public class OmniLookupJoinWithExprOperatorFactory
        extends OmniOperatorFactory<OmniLookupJoinWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni lookup join with expression operator factory.
     *
     * @param probeTypes the probe input types
     * @param probeOutputCols the probe output columns
     * @param probeHashKeys the probe hash keys
     * @param buildOutputCols the build output columns
     * @param buildOutputTypes the build output column types
     * @param joinType the join type
     * @param hashBuilderWithExprOperatorFactory the hash builder operator factory
     */
    public OmniLookupJoinWithExprOperatorFactory(
            VecType[] probeTypes,
            int[] probeOutputCols,
            String[] probeHashKeys,
            int[] buildOutputCols,
            VecType[] buildOutputTypes,
            JoinType joinType,
            OmniHashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory) {
        super(
                new FactoryContext(
                        new JitContext(
                                probeTypes,
                                probeOutputCols,
                                probeHashKeys,
                                buildOutputCols,
                                buildOutputTypes,
                                joinType),
                        hashBuilderWithExprOperatorFactory));
    }

    private static native long createLookupJoinWithExprOperatorFactory(
            String probeTypes,
            int[] probeOutputCols,
            String[] probeHashKeys,
            int[] buildOutputCols,
            String buildOutputTypes,
            int joinType,
            long hashBuilderWithExprOperatorFactory,
            long jitContext);

    private static native long createLookupJoinWithExprJitContext(
            String probeTypes,
            int[] probeOutputCols,
            String[] probeHashKeys,
            int[] buildOutputCols,
            String buildOutputTypes,
            int joinType);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createLookupJoinWithExprOperatorFactory(
                VecTypeSerializer.serialize(context.probeTypes),
                context.probeOutputCols,
                context.probeHashKeys,
                context.buildOutputCols,
                VecTypeSerializer.serialize(context.buildOutputTypes),
                context.joinType.getValue(),
                factoryContext.getHashBuilderWithExprOperatorFactory(),
                factoryContext.getNativeJitContext());
    }

    /**
     * The jit Context.
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] probeTypes;

        private final int[] probeOutputCols;

        private final String[] probeHashKeys;

        private final int[] buildOutputCols;

        private final VecType[] buildOutputTypes;

        private final JoinType joinType;

        public JitContext(
                VecType[] probeTypes,
                int[] probeOutputCols,
                String[] probeHashKeys,
                int[] buildOutputCols,
                VecType[] buildOutputTypes,
                JoinType joinType) {
            this.probeTypes = requireNonNull(probeTypes, "probeTypes");
            this.probeOutputCols = requireNonNull(probeOutputCols, "probeOutputCols");
            this.probeHashKeys = requireNonNull(probeHashKeys, "probeHashKeys");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes");
            this.joinType = requireNonNull(joinType, "joinType");
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    Arrays.hashCode(probeTypes),
                    Arrays.hashCode(probeOutputCols),
                    Arrays.hashCode(probeHashKeys),
                    Arrays.hashCode(buildOutputCols),
                    Arrays.hashCode(buildOutputTypes),
                    joinType);
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
            return joinType.equals(that.joinType)
                    && Arrays.equals(probeTypes, that.probeTypes)
                    && Arrays.equals(probeOutputCols, that.probeOutputCols)
                    && Arrays.equals(probeHashKeys, that.probeHashKeys)
                    && Arrays.equals(buildOutputCols, that.buildOutputCols)
                    && Arrays.equals(buildOutputTypes, that.buildOutputTypes);
        }
    }

    /**
     * The Factory context.
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        private final long hashBuilderWithExprOperatorFactory;

        public FactoryContext(
                JitContext jitContext, OmniHashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory) {
            super(jitContext);
            setNeedCache(false);
            this.hashBuilderWithExprOperatorFactory = hashBuilderWithExprOperatorFactory.getNativeOperatorFactory();
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return createLookupJoinWithExprJitContext(
                    VecTypeSerializer.serialize(context.probeTypes),
                    context.probeOutputCols,
                    context.probeHashKeys,
                    context.buildOutputCols,
                    VecTypeSerializer.serialize(context.buildOutputTypes),
                    context.joinType.getValue());
        }

        public long getHashBuilderWithExprOperatorFactory() {
            return hashBuilderWithExprOperatorFactory;
        }
    }
}
