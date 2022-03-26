/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

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
     */
    public OmniLimitOperatorFactory(long limit) {
        super(new FactoryContext(new JitContext(limit)));
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
    public static class JitContext implements OmniJitContext {
        private long limit;

        public JitContext(long limit) {
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
            return this.limit == context.limit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.limit);
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
