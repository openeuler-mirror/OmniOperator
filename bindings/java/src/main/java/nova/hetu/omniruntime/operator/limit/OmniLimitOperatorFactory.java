/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

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
        super(new FactoryContext(limit));
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

        /**
         * Instantiates a new Context.
         *
         * @param limit the limit count
         */
        public FactoryContext(long limit) {
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
            FactoryContext context = (FactoryContext) obj;
            return limit == context.limit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.limit);
        }
    }
}
