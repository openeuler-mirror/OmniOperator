/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.project;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni project operator factory.
 *
 * @since 2021-06-30
 */
public class OmniProjectOperatorFactory extends OmniOperatorFactory<OmniProjectOperatorFactory.FactoryContext> {
    private boolean isSupported;

    /**
     * Instantiates a new Omni project operator factory.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     * @param operatorConfig the operator config
     */
    public OmniProjectOperatorFactory(String[] expressions, DataType[] inputTypes, OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(expressions, inputTypes, operatorConfig)));
    }

    /**
     * Instantiates a new Omni project operator factory with default operator
     * config.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     */
    public OmniProjectOperatorFactory(String[] expressions, DataType[] inputTypes) {
        this(expressions, inputTypes, new OperatorConfig(true));
    }

    /**
     * Instantiates a new Omni project operator factory with configured expression
     * parsing format.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     * @param parseFormat the parse format
     * @param operatorConfig the operator config
     */
    public OmniProjectOperatorFactory(String[] expressions, DataType[] inputTypes, int parseFormat,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(expressions, inputTypes, parseFormat, operatorConfig)));
    }

    /**
     * Instantiates a new Omni project operator factory with configured expression
     * parsing format with default operator config.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     * @param parseFormat the parse format
     */
    public OmniProjectOperatorFactory(String[] expressions, DataType[] inputTypes, int parseFormat) {
        this(expressions, inputTypes, parseFormat, new OperatorConfig(true));
    }

    private static native long createProjectOperatorFactory(String inputTypes, int inputLength, Object[] expressions,
            int expressionsLength, long jitContext, int parseFormat, boolean isSkipVerify);

    private static native long createProjectJitContext(String inputTypes, int inputLength, Object[] expressions,
            int expressionsLength);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        long factoryAddr = createProjectOperatorFactory(DataTypeSerializer.serialize(context.inputTypes),
                context.inputTypes.length, context.expressions, context.expressions.length,
                factoryContext.getNativeJitContext(), context.parseFormat,
                context.getOperatorConfig().isSkipExpressionVerify());
        if (factoryAddr != 0) {
            isSupported = true;
        }
        return factoryAddr;
    }

    public boolean isSupported() {
        return isSupported;
    }

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext extends OmniJitContext {
        private final DataType[] inputTypes;

        private final String[] expressions;

        private final int parseFormat;

        /**
         * Instantiates a new Context.
         *
         * @param expressions the expressions
         * @param inputTypes the input types
         * @param operatorConfig the operator config
         */
        public JitContext(String[] expressions, DataType[] inputTypes, OperatorConfig operatorConfig) {
            this(expressions, inputTypes, 0, operatorConfig);
        }

        /**
         * Instantiates a new Context with configured parsing format of the expression.
         *
         * @param expressions the expressions
         * @param inputTypes the input types
         * @param parseFormat the parse format
         * @param operatorConfig the operator config
         */
        public JitContext(String[] expressions, DataType[] inputTypes, int parseFormat, OperatorConfig operatorConfig) {
            super(operatorConfig);
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expressions = requireNonNull(expressions, "Expressions is null.");
            this.parseFormat = parseFormat;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(expressions), Arrays.hashCode(inputTypes), operatorConfig);
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
            return Arrays.equals(expressions, that.expressions) && Arrays.equals(inputTypes, that.inputTypes)
                    && parseFormat == that.parseFormat && operatorConfig.equals(that.operatorConfig);
        }
    }

    /**
     * The type Factory context.
     *
     * @since 2021-06-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         */
        public FactoryContext(JitContext jitContext) {
            super(jitContext);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return 0;
        }
    }
}
