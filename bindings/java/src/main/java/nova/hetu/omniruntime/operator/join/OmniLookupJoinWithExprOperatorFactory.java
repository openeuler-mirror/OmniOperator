/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

/**
 * The Omni lookup join with expression operator factory.
 *
 * @since 2021-10-16
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
     * @param operatorConfig the operator config
     */
    public OmniLookupJoinWithExprOperatorFactory(DataType[] probeTypes, int[] probeOutputCols, String[] probeHashKeys,
            int[] buildOutputCols, DataType[] buildOutputTypes, JoinType joinType,
            OmniHashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory, OperatorConfig operatorConfig) {
        super(new FactoryContext(probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes,
                joinType, operatorConfig, hashBuilderWithExprOperatorFactory));
    }

    /**
     * Instantiates a new Omni lookup join with expression operator factory with
     * default operator config.
     *
     * @param probeTypes the probe input types
     * @param probeOutputCols the probe output columns
     * @param probeHashKeys the probe hash keys
     * @param buildOutputCols the build output columns
     * @param buildOutputTypes the build output column types
     * @param joinType the join type
     * @param hashBuilderWithExprOperatorFactory the hash builder operator factory
     */
    public OmniLookupJoinWithExprOperatorFactory(DataType[] probeTypes, int[] probeOutputCols, String[] probeHashKeys,
            int[] buildOutputCols, DataType[] buildOutputTypes, JoinType joinType,
            OmniHashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory) {
        this(probeTypes, probeOutputCols, probeHashKeys, buildOutputCols, buildOutputTypes, joinType,
                hashBuilderWithExprOperatorFactory, new OperatorConfig());
    }

    private static native long createLookupJoinWithExprOperatorFactory(String probeTypes, int[] probeOutputCols,
            String[] probeHashKeys, int[] buildOutputCols, String buildOutputTypes, int joinType,
            long hashBuilderWithExprOperatorFactory);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createLookupJoinWithExprOperatorFactory(DataTypeSerializer.serialize(context.probeTypes),
                context.probeOutputCols, context.probeHashKeys, context.buildOutputCols,
                DataTypeSerializer.serialize(context.buildOutputTypes), context.joinType.getValue(),
                context.getHashBuilderWithExprOperatorFactory());
    }

    /**
     * The Factory context.
     *
     * @since 2021-10-16
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final DataType[] probeTypes;

        private final int[] probeOutputCols;

        private final String[] probeHashKeys;

        private final int[] buildOutputCols;

        private final DataType[] buildOutputTypes;

        private final JoinType joinType;

        private final OperatorConfig operatorConfig;

        private final long hashBuilderWithExprOperatorFactory;

        /**
         * Instantiates a new Context.
         *
         * @param probeTypes the probe types
         * @param probeOutputCols the probe output cols
         * @param probeHashKeys the probe hash keys
         * @param buildOutputCols the build output cols
         * @param buildOutputTypes the build output types
         * @param joinType the join type
         * @param operatorConfig the operator config
         * @param hashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory
         */
        public FactoryContext(DataType[] probeTypes, int[] probeOutputCols, String[] probeHashKeys,
                int[] buildOutputCols, DataType[] buildOutputTypes, JoinType joinType, OperatorConfig operatorConfig,
                OmniHashBuilderWithExprOperatorFactory hashBuilderWithExprOperatorFactory) {
            this.probeTypes = requireNonNull(probeTypes, "probeTypes");
            this.probeOutputCols = requireNonNull(probeOutputCols, "probeOutputCols");
            this.probeHashKeys = requireNonNull(probeHashKeys, "probeHashKeys");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes");
            this.joinType = requireNonNull(joinType, "joinType");
            this.operatorConfig = operatorConfig;
            this.hashBuilderWithExprOperatorFactory = hashBuilderWithExprOperatorFactory.getNativeOperatorFactory();
            setNeedCache(false);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(probeTypes), Arrays.hashCode(probeOutputCols),
                    Arrays.hashCode(probeHashKeys), Arrays.hashCode(buildOutputCols), Arrays.hashCode(buildOutputTypes),
                    joinType, operatorConfig);
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
            return joinType.equals(that.joinType) && Arrays.equals(probeTypes, that.probeTypes)
                    && Arrays.equals(probeOutputCols, that.probeOutputCols)
                    && Arrays.equals(probeHashKeys, that.probeHashKeys)
                    && Arrays.equals(buildOutputCols, that.buildOutputCols)
                    && Arrays.equals(buildOutputTypes, that.buildOutputTypes)
                    && operatorConfig.equals(that.operatorConfig);
        }

        public long getHashBuilderWithExprOperatorFactory() {
            return hashBuilderWithExprOperatorFactory;
        }
    }
}
