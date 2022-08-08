/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

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
        super(new FactoryContext(limit, operatorConfig));
    }

    /**
     * Instantiates a new Omni limit operator factory with default operator config.
     *
     * @param limit the limit count
     */
    public OmniLimitOperatorFactory(long limit) {
        this(limit, new OperatorConfig());
    }

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createLimitOperatorFactory(context.limit);
    }

    private static native long createLimitOperatorFactory(long limit);

    /**
     * The type Factory context.
     *
     * @since 2021-06-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final long limit;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param limit the limit count
         * @param operatorConfig the operator config
         */
        public FactoryContext(long limit, OperatorConfig operatorConfig) {
            this.limit = limit;
            this.operatorConfig = operatorConfig;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FactoryContext context = (FactoryContext) obj;
            return limit == context.limit && operatorConfig.equals(context.operatorConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.limit, operatorConfig);
        }
    }
}
