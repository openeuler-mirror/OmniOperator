/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.union;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni union operator factory.
 *
 * @since 2021-06-30
 */
public class OmniUnionOperatorFactory extends OmniOperatorFactory<OmniUnionOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni union operator factory.
     *
     * @param sourceTypes the source type
     * @param isDistinct mark union or union all
     * @param operatorConfig the operator config
     */
    public OmniUnionOperatorFactory(DataType[] sourceTypes, boolean isDistinct, OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(sourceTypes, isDistinct, operatorConfig)));
    }

    /**
     * Instantiates a new Omni union operator factory with default operator config.
     *
     * @param sourceTypes the source type
     * @param isDistinct mark union or union all
     */
    public OmniUnionOperatorFactory(DataType[] sourceTypes, boolean isDistinct) {
        this(sourceTypes, isDistinct, new OperatorConfig(true));
    }

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createUnionOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.isDistinct,
                factoryContext.getNativeJitContext());
    }

    private static native long createUnionOperatorFactory(String sourceTypes, boolean isDistinct, long jitcontext);

    private static native long createUnionJitContext(String sourceTypes, boolean isDistinct);

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext extends OmniJitContext {
        private final DataType[] sourceTypes;

        private final boolean isDistinct;

        /**
         * Instantiates a new Jit context.
         *
         * @param sourceTypes the source types
         * @param isDistinct the is distinct
         * @param operatorConfig the operator config
         */
        public JitContext(DataType[] sourceTypes, boolean isDistinct, OperatorConfig operatorConfig) {
            super(operatorConfig);
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
            return isDistinct == context.isDistinct && Arrays.equals(sourceTypes, context.sourceTypes)
                    && operatorConfig.equals(context.operatorConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), isDistinct, operatorConfig);
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
