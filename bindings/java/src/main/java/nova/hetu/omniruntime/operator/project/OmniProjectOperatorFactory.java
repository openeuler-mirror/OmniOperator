/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.project;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni project operator factory.
 *
 * @since 20210630
 */
public class OmniProjectOperatorFactory extends OmniOperatorFactory<OmniProjectOperatorFactory.FactoryContext> {
    private boolean isSupported;

    /**
     * Instantiates a new Omni project operator factory.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     */
    public OmniProjectOperatorFactory(String[] expressions, VecType[] inputTypes) {
        super(new FactoryContext(new JitContext(expressions, inputTypes)));
    }

    private static native long createProjectOperatorFactory(String inputTypes, int inputLength, Object[] expressions,
        int expressionsLength, long jitContext);

    private static native long createProjectJitContext(String inputTypes, int inputLength, Object[] expressions,
        int expressionsLength);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        long factoryAddr = createProjectOperatorFactory(VecTypeSerializer.serialize(context.inputTypes), context.inputTypes.length,
            context.expressions, context.expressions.length, factoryContext.getNativeJitContext());
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
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] inputTypes;

        private final String[] expressions;

        /**
         * Instantiates a new Context.
         *
         * @param expressions the expressions
         * @param inputTypes the input types
         */
        public JitContext(String[] expressions, VecType[] inputTypes) {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expressions = requireNonNull(expressions, "Expressions is null.");
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(expressions), Arrays.hashCode(inputTypes));
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
            return Arrays.equals(expressions, that.expressions) && Arrays.equals(inputTypes, that.inputTypes);
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
            //todo: use createProjectJitContext when there is a jit optimization in future.
            // return createProjectJitContext(
            //     VecTypeSerializer.serialize(context.inputTypes), context.inputTypes.length,
            //     context.expressions, context.expressions.length);
            return 0;
        }
    }
}
