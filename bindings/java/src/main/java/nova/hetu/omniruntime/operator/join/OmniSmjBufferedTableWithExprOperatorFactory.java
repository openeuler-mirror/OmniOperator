/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

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
 * The type Omni sort merge buffered table with expression operator factory.
 *
 * @since 2021-10-30
 */
public class OmniSmjBufferedTableWithExprOperatorFactory
        extends OmniOperatorFactory<OmniSmjBufferedTableWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni sort merge buffered table factory.
     *
     * @param soruceTypes the all input vector types
     * @param equalKeyExprs equal condition key expressions
     * @param outputChannels output of streamed table
     * @param smjStreamedTableOperatorFactory streamed table operator factory
     *            instance
     * @param operatorConfig the operator config
     */
    public OmniSmjBufferedTableWithExprOperatorFactory(DataType[] soruceTypes, String[] equalKeyExprs,
            int[] outputChannels, OmniSmjStreamedTableWithExprOperatorFactory smjStreamedTableOperatorFactory,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(soruceTypes, equalKeyExprs, outputChannels, operatorConfig),
                smjStreamedTableOperatorFactory));
    }

    /**
     * Instantiates a new Omni sort merge buffered table factory with default
     * operator config.
     *
     * @param soruceTypes the all input vector types
     * @param equalKeyExprs equal condition key expressions
     * @param outputChannels output of streamed table
     * @param smjStreamedTableOperatorFactory streamed table operator factory
     *            instance
     */
    public OmniSmjBufferedTableWithExprOperatorFactory(DataType[] soruceTypes, String[] equalKeyExprs,
            int[] outputChannels, OmniSmjStreamedTableWithExprOperatorFactory smjStreamedTableOperatorFactory) {
        this(soruceTypes, equalKeyExprs, outputChannels, smjStreamedTableOperatorFactory, new OperatorConfig(true));
    }

    private static native long createSmjBufferedTableWithExprOperatorFactory(String soruceTypes, String[] equalKeyExprs,
            int[] outputChannels, long smjStreamedTableOperatorFactory, long jitContext);

    private static native long createSmjBufferedTableWithExprJitContext(String soruceTypes, String[] equalKeyExprs,
            int[] outputChannels);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createSmjBufferedTableWithExprOperatorFactory(DataTypeSerializer.serialize(context.soruceTypes),
                context.equalKeyExprs, context.outputChannels, factoryContext.getStreamedTableOperatorFactory(),
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 2021-10-30
     */
    public static class JitContext extends OmniJitContext {
        private final DataType[] soruceTypes;

        private final String[] equalKeyExprs;

        private final int[] outputChannels;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param soruceTypes the all input vector types
         * @param equalKeyExps equal condition key expressions
         * @param outputChannels output of streamed table
         * @param operatorConfig the operator config
         */
        public JitContext(DataType[] soruceTypes, String[] equalKeyExps, int[] outputChannels,
                OperatorConfig operatorConfig) {
            super(operatorConfig);
            this.soruceTypes = requireNonNull(soruceTypes, "soruceTypes");
            this.equalKeyExprs = requireNonNull(equalKeyExps, "equalKeyExprs");
            this.outputChannels = requireNonNull(outputChannels, "outputChannels");
            this.operatorConfig = requireNonNull(operatorConfig, "operatorConfig");
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(soruceTypes), Arrays.hashCode(equalKeyExprs),
                    Arrays.hashCode(outputChannels), operatorConfig);
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
            return Arrays.equals(soruceTypes, that.soruceTypes) && Arrays.equals(equalKeyExprs, that.equalKeyExprs)
                    && Arrays.equals(outputChannels, that.outputChannels) && operatorConfig.equals(that.operatorConfig);
        }
    }

    /**
     * The type Factory context.
     *
     * @since 2021-10-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        private final long streamedTableOperatorFactory;

        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         * @param streamedTableOperatorFactory streamed table operator factory instance
         */
        public FactoryContext(JitContext jitContext,
                OmniSmjStreamedTableWithExprOperatorFactory streamedTableOperatorFactory) {
            super(jitContext);
            setNeedCache(false);
            this.streamedTableOperatorFactory = streamedTableOperatorFactory.getNativeOperatorFactory();
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return createSmjBufferedTableWithExprJitContext(DataTypeSerializer.serialize(context.soruceTypes),
                    context.equalKeyExprs, context.outputChannels);
        }

        public long getStreamedTableOperatorFactory() {
            return streamedTableOperatorFactory;
        }
    }
}
