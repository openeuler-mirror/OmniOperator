/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
 * The Omni hash builder with expression operator factory.
 *
 * @since 2021-10-16
 */
public class OmniHashBuilderWithExprOperatorFactory
        extends OmniOperatorFactory<OmniHashBuilderWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni hash builder with expression operator factory.
     *
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param filterExpression the filter expression in join
     * @param operatorCount the operator count
     * @param isJitEnabled whether the jit is enabled
     */
    public OmniHashBuilderWithExprOperatorFactory(DataType[] buildTypes, String[] buildHashKeys,
            Optional<String> filterExpression, int operatorCount, boolean isJitEnabled) {
        super(new FactoryContext(new JitContext(buildTypes, buildHashKeys, filterExpression, operatorCount),
                isJitEnabled));
    }

    /**
     * Instantiates a new Omni hash builder with expression operator factory with
     * jit default.
     *
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param filterExpression the filter expression in join
     * @param operatorCount the operator count
     */
    public OmniHashBuilderWithExprOperatorFactory(DataType[] buildTypes, String[] buildHashKeys,
            Optional<String> filterExpression, int operatorCount) {
        this(buildTypes, buildHashKeys, filterExpression, operatorCount, true);
    }

    private static native long createHashBuilderWithExprOperatorFactory(String buildTypes, String[] buildHashKeys,
            String filterExpression, int operatorCount, long jitContext);

    private static native long createHashBuilderWithExprJitContext(String buildTypes, String[] buildHashKeys,
            String filterExpression, int operatorCount);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createHashBuilderWithExprOperatorFactory(DataTypeSerializer.serialize(context.buildTyeps),
                context.buildHashKeys, context.filterExpression, context.operatorCount,
                factoryContext.getNativeJitContext());
    }

    /**
     * The jit Context.
     *
     * @since 2021-10-16
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] buildTyeps;

        private final String[] buildHashKeys;

        private final String filterExpression;

        private final int operatorCount;

        public JitContext(DataType[] buildTyeps, String[] buildHashKeys, Optional<String> filterExpression,
                int operatorCount) {
            this.buildTyeps = requireNonNull(buildTyeps, "buildTyeps");
            this.buildHashKeys = requireNonNull(buildHashKeys, "buildHashKeys");
            this.filterExpression = filterExpression.isPresent() ? filterExpression.get() : "";
            this.operatorCount = operatorCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(buildTyeps), Arrays.hashCode(buildHashKeys), filterExpression,
                    operatorCount);
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
            return Arrays.equals(buildTyeps, that.buildTyeps) && Arrays.equals(buildHashKeys, that.buildHashKeys)
                    && filterExpression.equals(that.filterExpression) && operatorCount == that.operatorCount;
        }
    }

    /**
     * The Factory context.
     *
     * @since 2021-10-16
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
            return createHashBuilderWithExprJitContext(DataTypeSerializer.serialize(context.buildTyeps),
                    context.buildHashKeys, context.filterExpression, context.operatorCount);
        }
    }
}
