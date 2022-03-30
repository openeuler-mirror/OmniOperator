/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The type Omni hash builder operator factory.
 *
 * @since 2021-06-30
 */
public class OmniHashBuilderOperatorFactory extends OmniOperatorFactory<OmniHashBuilderOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash builder operator factory.
     *
     * @param buildTypes the build types
     * @param buildHashCols the build hash cols
     * @param filterExpression the join filter expression
     * @param sortChannel the sort channel
     * @param searchExpressions the search expressions
     * @param operatorCount the operator count
     */
    public OmniHashBuilderOperatorFactory(DataType[] buildTypes, int[] buildHashCols, Optional<String> filterExpression,
            Optional<Integer> sortChannel, String[] searchExpressions, int operatorCount) {
        super(new FactoryContext(new JitContext(buildTypes, buildHashCols, filterExpression, sortChannel,
                searchExpressions, operatorCount)));
    }

    private static native long createHashBuilderOperatorFactory(String buildTypes, int[] buildHashCols,
            String filterExpression, int sortChannel, String[] searchExpressions, int operatorCount, long jitContext);

    private static native long createHashBuilderJitContext(String buildTypes, int[] buildHashCols,
            String filterExpression, int sortChannel, String[] searchExpressions, int operatorCount);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashBuilderOperatorFactory(DataTypeSerializer.serialize(context.buildTypes), context.buildHashCols,
                context.filterExpression, context.sortChannel, context.searchExpressions, context.operatorCount,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] buildTypes;

        private final int[] buildHashCols;

        private final String filterExpression;

        private final Integer sortChannel;

        private final String[] searchExpressions;

        private final int operatorCount;

        /**
         * Instantiates a new Context.
         *
         * @param buildTypes the build types
         * @param buildHashCols the build hash cols
         * @param filterExpression the join filter expression
         * @param sortChannel the sort channel
         * @param searchExpressions the search expressions
         * @param operatorCount the operator count
         */
        public JitContext(DataType[] buildTypes, int[] buildHashCols, Optional<String> filterExpression,
                Optional<Integer> sortChannel, String[] searchExpressions, int operatorCount) {
            this.buildTypes = requireNonNull(buildTypes, "buildTypes");
            this.buildHashCols = requireNonNull(buildHashCols, "buildHashCols");
            this.filterExpression = filterExpression.isPresent() ? filterExpression.get() : "";
            this.sortChannel = sortChannel.isPresent() ? sortChannel.get() : -1;
            this.searchExpressions = searchExpressions;
            this.operatorCount = operatorCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(buildTypes), Arrays.hashCode(buildHashCols), filterExpression,
                    sortChannel, Arrays.hashCode(searchExpressions), operatorCount);
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
            return Arrays.equals(buildTypes, that.buildTypes) && Arrays.equals(buildHashCols, that.buildHashCols)
                    && filterExpression.equals(that.filterExpression) && sortChannel.equals(sortChannel)
                    && Arrays.equals(searchExpressions, that.searchExpressions) && operatorCount == that.operatorCount;
        }
    }

    /**
     * The type Factory context.
     *
     * @since 20210630
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
            return createHashBuilderJitContext(DataTypeSerializer.serialize(context.buildTypes), context.buildHashCols,
                    context.filterExpression, context.sortChannel, context.searchExpressions, context.operatorCount);
        }
    }
}
