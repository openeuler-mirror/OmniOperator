/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.union;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni union operator factory.
 *
 * @since 20210630
 */
public class OmniUnionOperatorFactory extends OmniOperatorFactory<OmniUnionOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni union operator factory.
     *
     * @param sourceTypes the source type
     * @param isDistinct mark union or union all
     */
    public OmniUnionOperatorFactory(VecType[] sourceTypes, boolean isDistinct) {
        super(new FactoryContext(new JitContext(sourceTypes, isDistinct)));
    }

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createUnionOperatorFactory(VecTypeSerializer.serialize(context.sourceTypes), context.isDistinct,
            factoryContext.getNativeJitContext());
    }

    private static native long createUnionOperatorFactory(String sourceTypes, boolean isDistinct, long jitcontext);

    private static native long createUnionJitContext(String sourceTypes, boolean isDistinct);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] sourceTypes;

        private final boolean isDistinct;

        /**
         * Instantiates a new Jit context.
         *
         * @param sourceTypes the source types
         * @param isDistinct the is distinct
         */
        public JitContext(VecType[] sourceTypes, boolean isDistinct) {
            this.sourceTypes = sourceTypes;
            this.isDistinct = isDistinct;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext context = null;
            if (obj instanceof JitContext) {
                context = (JitContext) obj;
            }
            return isDistinct == context.isDistinct && Arrays.equals(sourceTypes, context.sourceTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), isDistinct);
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
            //todo: use createUnionJitContext when there is a jit optimization in future.
            // return createUnionJitContext(
            //     VecTypeSerializer.serialize(context.sourceTypes), context.isDistinct);
            return 0;
        }
    }
}
