/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.filter;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The type Omni filter and project operator factory.
 *
 * @since 2021-06-30
 */
public class OmniFilterAndProjectOperatorFactory
        extends OmniOperatorFactory<OmniFilterAndProjectOperatorFactory.FactoryContext> {
    private boolean isSupported;

    /**
     * Instantiates a new Omni filter and project operator factory.
     *
     * @param expression the expression
     * @param inputTypes the input types
     * @param projections the projections
     * @param operatorConfig the operator config
     */
    public OmniFilterAndProjectOperatorFactory(String expression, DataType[] inputTypes, List<String> projections,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(expression, inputTypes, projections, operatorConfig)));
    }

    /**
     * Instantiates a new Omni filter and project operator factory with default
     * operator config.
     *
     * @param expression the expression
     * @param inputTypes the input types
     * @param projections the projections
     */
    public OmniFilterAndProjectOperatorFactory(String expression, DataType[] inputTypes, List<String> projections) {
        this(expression, inputTypes, projections, new OperatorConfig(true));
    }

    /**
     * Instantiates a new Omni filter and project operator factory with configured
     * expression parsing format.
     *
     * @param expression the expression
     * @param inputTypes the input types
     * @param projections the projections
     * @param parseFormat the format to parse expression into
     * @param operatorConfig the operator config
     */
    public OmniFilterAndProjectOperatorFactory(String expression, DataType[] inputTypes, List<String> projections,
            int parseFormat, OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(expression, inputTypes, projections, parseFormat, operatorConfig)));
    }

    /**
     * Instantiates a new Omni filter and project operator factory with configured
     * expression parsing format with default operator config.
     *
     * @param expression the expression
     * @param inputTypes the input types
     * @param projections the projections
     * @param parseFormat the format to parse expression into
     */
    public OmniFilterAndProjectOperatorFactory(String expression, DataType[] inputTypes, List<String> projections,
            int parseFormat) {
        this(expression, inputTypes, projections, parseFormat, new OperatorConfig(true));
    }

    private static native long createFilterAndProjectOperatorFactory(String inputTypes, int inputLength,
            String expression, Object[] projections, int projectLength, long jitContext, int parseFormat,
            boolean isSkipVerify);

    private static native long createFilterAndProjectJitContext(String inputTypes, int inputLength, String expression,
            int[] projectIndices, int projectLength);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        // long nativeOperatorFactory is 0 if operations/data-types are unsupported
        JitContext context = factoryContext.getJitContext();
        long factoryAddr = createFilterAndProjectOperatorFactory(DataTypeSerializer.serialize(context.inputTypes),
                context.inputTypes.length, context.expression, context.projections.toArray(),
                context.projections.size(), factoryContext.getNativeJitContext(), context.parseFormat,
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

        private final String expression;

        private final List<String> projections;

        private final int parseFormat;

        /**
         * Instantiates a new Context.
         *
         * @param expression the expression
         * @param inputTypes the input types
         * @param projections the projections
         * @param operatorConfig the operator config
         */
        public JitContext(String expression, DataType[] inputTypes, List<String> projections,
                OperatorConfig operatorConfig) {
            this(expression, inputTypes, projections, 0, operatorConfig);
        }

        /**
         * Instantiates a new Context with configured parsing format of the expression.
         *
         * @param expression the expression
         * @param inputTypes the input types
         * @param projections the projections
         * @param parseFormat the parsing format of expressions
         * @param operatorConfig the operator config
         */
        public JitContext(String expression, DataType[] inputTypes, List<String> projections, int parseFormat,
                OperatorConfig operatorConfig) {
            super(operatorConfig);
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expression = requireNonNull(expression, "Expression is null.");
            this.projections = requireNonNull(projections, "Project indices is null.");
            this.parseFormat = parseFormat;
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression, Arrays.hashCode(inputTypes), Objects.hashCode(projections), parseFormat,
                    operatorConfig);
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
            return Objects.equals(expression, that.expression) && Arrays.equals(inputTypes, that.inputTypes)
                    && Objects.equals(projections.stream().sorted().collect(Collectors.toList()),
                            that.projections.stream().sorted().collect(Collectors.toList()))
                    && parseFormat == that.parseFormat && operatorConfig.equals(that.operatorConfig);
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
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return 0;
        }
    }
}
