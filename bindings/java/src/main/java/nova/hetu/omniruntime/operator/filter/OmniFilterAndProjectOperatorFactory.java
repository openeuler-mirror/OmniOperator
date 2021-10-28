/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.filter;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The type Omni filter and project operator factory.
 *
 * @since 20210630
 */
public class OmniFilterAndProjectOperatorFactory
    extends OmniOperatorFactory<OmniFilterAndProjectOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni filter and project operator factory.
     *
     * @param expression the expression
     * @param inputTypes the input types
     * @param projections the projections
     */
    public OmniFilterAndProjectOperatorFactory(String expression, VecType[] inputTypes, List<String> projections) {
        super(new FactoryContext(new JitContext(expression, inputTypes, projections)));
    }

    private static native long createFilterAndProjectOperatorFactory(String inputTypes, int inputLength,
        String expression, Object[] projections, int projectLength, long jitContext);

    private static native long createFilterAndProjectJitContext(String inputTypes, int inputLength, String expression,
        int[] projectIndices, int projectLength);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        // long nativeOperatorFactory is 0 if operations/data-types are unsupported
        JitContext context = factoryContext.getJitContext();
        return createFilterAndProjectOperatorFactory(VecTypeSerializer.serialize(context.inputTypes),
            context.inputTypes.length, context.expression, context.projections.toArray(), context.projections.size(),
            factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] inputTypes;

        private final String expression;

        private final List<String> projections;

        /**
         * Instantiates a new Context.
         *
         * @param expression the expression
         * @param inputTypes the input types
         * @param projections the projections
         */
        public JitContext(String expression, VecType[] inputTypes, List<String> projections) {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expression = requireNonNull(expression, "Expression is null.");
            this.projections = requireNonNull(projections, "Project indices is null.");
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression, Arrays.hashCode(inputTypes), Objects.hashCode(projections));
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
                that.projections.stream().sorted().collect(Collectors.toList()));
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
            //todo: use createProjectAndProjectJitContext when there is a jit optimization in future.
            // return createFilterAndProjectJitContext(VecTypeSerializer.serialize(context.inputTypes),
            //     context.inputTypes.length, context.expression, context.projectIndices, context.projectIndices.length);
            return 0;
        }
    }
}
