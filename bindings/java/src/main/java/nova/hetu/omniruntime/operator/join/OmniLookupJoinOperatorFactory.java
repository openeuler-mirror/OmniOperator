/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni lookup join operator factory.
 *
 * @since 20210630
 */
public class OmniLookupJoinOperatorFactory extends OmniOperatorFactory<OmniLookupJoinOperatorFactory.Context> {
    /**
     * Instantiates a new Omni lookup join operator factory.
     *
     * @param probeTypes                 the probe types
     * @param probeOutputCols            the probe output cols
     * @param probeHashCols              the probe hash cols
     * @param buildOutputCols            the build output cols
     * @param buildOutputTypes           the build output types
     * @param joinType                   the join type
     * @param hashBuilderOperatorFactory the hash builder operator factory
     */
    public OmniLookupJoinOperatorFactory(
            VecType[] probeTypes,
            int[] probeOutputCols,
            int[] probeHashCols,
            int[] buildOutputCols,
            VecType[] buildOutputTypes,
            JoinType joinType,
            OmniHashBuilderOperatorFactory hashBuilderOperatorFactory) {
        super(
                new Context(
                        probeTypes,
                        probeOutputCols,
                        probeHashCols,
                        buildOutputCols,
                        buildOutputTypes,
                        joinType,
                        hashBuilderOperatorFactory));
    }

    private static native long createLookupJoinOperatorFactory(
            String probeTypes,
            int[] probeOutputCols,
            int[] probeHashCols,
            int[] buildOutputCols,
            String buildOutputTypes,
            int joinType,
            long hashBuilderOperatorFactory);

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createLookupJoinOperatorFactory(
                VecTypeSerializer.serialize(context.probeTypes),
                context.probeOutputCols,
                context.probeHashCols,
                context.buildOutputCols,
                VecTypeSerializer.serialize(context.buildOutputTypes),
                context.joinType.getValue(),
                context.hashBuilderOperatorFactory);
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] probeTypes;

        private final int[] probeOutputCols;

        private final int[] probeHashCols;

        private final int[] buildOutputCols;

        private final VecType[] buildOutputTypes;

        private final JoinType joinType;

        private final long hashBuilderOperatorFactory;

        /**
         * Instantiates a new Context.
         *
         * @param probeTypes                 the probe types
         * @param probeOutputCols            the probe output cols
         * @param probeHashCols              the probe hash cols
         * @param buildOutputCols            the build output cols
         * @param buildOutputTypes           the build output types
         * @param joinType                   the join type
         * @param hashBuilderOperatorFactory the hash builder operator factory
         */
        public Context(
                VecType[] probeTypes,
                int[] probeOutputCols,
                int[] probeHashCols,
                int[] buildOutputCols,
                VecType[] buildOutputTypes,
                JoinType joinType,
                OmniHashBuilderOperatorFactory hashBuilderOperatorFactory) {
            this.probeTypes = requireNonNull(probeTypes, "probeTypes");
            this.probeOutputCols = requireNonNull(probeOutputCols, "probeOutputCols");
            this.probeHashCols = requireNonNull(probeHashCols, "probeHashCols");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes");
            this.joinType = requireNonNull(joinType, "joinType");
            requireNonNull(hashBuilderOperatorFactory, "hashBuilderOperatorFactory");
            this.hashBuilderOperatorFactory = hashBuilderOperatorFactory.getNativeOperatorFactory();
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    Arrays.hashCode(probeTypes),
                    Arrays.hashCode(probeOutputCols),
                    Arrays.hashCode(probeHashCols),
                    Arrays.hashCode(buildOutputCols),
                    Arrays.hashCode(buildOutputTypes),
                    joinType,
                    hashBuilderOperatorFactory);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Context that = (Context) obj;
            return hashBuilderOperatorFactory == that.hashBuilderOperatorFactory
                    && joinType.equals(that.joinType)
                    && Arrays.equals(probeTypes, that.probeTypes)
                    && Arrays.equals(probeOutputCols, that.probeOutputCols)
                    && Arrays.equals(probeHashCols, that.probeHashCols)
                    && Arrays.equals(buildOutputCols, that.buildOutputCols)
                    && Arrays.equals(buildOutputTypes, that.buildOutputTypes);
        }
    }
}
