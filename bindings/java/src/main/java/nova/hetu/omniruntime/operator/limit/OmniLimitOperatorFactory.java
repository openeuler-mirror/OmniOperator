/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;

import java.util.Objects;

/**
 * The type Omni limit operator factory.
 *
 * @since 2021-06-30
 */
public class OmniLimitOperatorFactory extends OmniOperatorFactory<OmniLimitOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni limit operator factory.
     *
     * @param limit the limit count
     * @param operatorConfig the operator config
     */
    public OmniLimitOperatorFactory(long limit, OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(limit, operatorConfig)));
    }

    /**
     * Instantiates a new Omni limit operator factory with default operator config.
     *
     * @param limit the limit count
     */
    public OmniLimitOperatorFactory(long limit) {
        this(limit, new OperatorConfig(true));
    }

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createLimitOperatorFactory(context.limit);
    }

    private static native long createLimitOperatorFactory(long limit);

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext extends OmniJitContext {
        private final long limit;

        /**
         * Instantiates a new Context.
         *
         * @param limit the limit count
         * @param operatorConfig the operator config
         */
        public JitContext(long limit, OperatorConfig operatorConfig) {
            super(operatorConfig);
            this.limit = limit;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext context = (JitContext) obj;
            return limit == context.limit && operatorConfig.equals(context.operatorConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.limit, operatorConfig);
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
