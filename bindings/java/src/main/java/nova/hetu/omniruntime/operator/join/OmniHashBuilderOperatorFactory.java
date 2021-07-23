/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni hash builder operator factory.
 *
 * @since 20210630
 */
public class OmniHashBuilderOperatorFactory extends OmniOperatorFactory<OmniHashBuilderOperatorFactory.Context> {
    /**
     * Instantiates a new Omni hash builder operator factory.
     *
     * @param buildTypes the build types
     * @param buildOutputCols the build output cols
     * @param buildHashCols the build hash cols
     * @param operatorCount the operator count
     */
    public OmniHashBuilderOperatorFactory(VecType[] buildTypes, int[] buildOutputCols, int[] buildHashCols,
        int operatorCount) {
        super(new Context(buildTypes, buildOutputCols, buildHashCols, operatorCount));
    }

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createHashBuilderOperatorFactory(toNativeConstants(context.buildTypes), context.buildOutputCols,
            context.buildHashCols, context.operatorCount);
    }

    private static native long createHashBuilderOperatorFactory(int[] buildTypes, int[] buildOutputCols,
        int[] buildHashCols, int operatorCount);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] buildTypes;

        private final int[] buildOutputCols;

        private final int[] buildHashCols;

        private final int operatorCount;

        /**
         * Instantiates a new Context.
         *
         * @param buildTypes the build types
         * @param buildOutputCols the build output cols
         * @param buildHashCols the build hash cols
         * @param operatorCount the operator count
         */
        public Context(VecType[] buildTypes, int[] buildOutputCols, int[] buildHashCols, int operatorCount) {
            this.buildTypes = requireNonNull(buildTypes, "buildTypes");
            this.buildOutputCols = requireNonNull(buildOutputCols, "buildOutputCols");
            this.buildHashCols = requireNonNull(buildHashCols, "buildHashCols");
            this.operatorCount = operatorCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(buildTypes), Arrays.hashCode(buildOutputCols),
                Arrays.hashCode(buildHashCols), operatorCount);
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
            return Arrays.equals(buildTypes, that.buildTypes) && Arrays.equals(buildOutputCols, that.buildOutputCols)
                && Arrays.equals(buildHashCols, that.buildHashCols) && operatorCount == that.operatorCount;
        }
    }
}
