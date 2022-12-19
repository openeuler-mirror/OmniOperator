/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package nova.hetu.omniruntime.operator.fusion;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.OperatorType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;

import java.util.Arrays;
import java.util.Objects;

public class OmniFusionOperatorFactory extends OmniOperatorFactory<OmniFusionOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni fusion operator factory.
     *
     * @param operatorFactories the operator factories
     * @param operatorTypes the operator types
     * @param operatorConfig the operator config
     */
    public OmniFusionOperatorFactory(OmniOperatorFactory<?>[] operatorFactories, OperatorType[] operatorTypes,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(operatorFactories, operatorTypes, operatorConfig));
    }

    /**
     * Instantiates a new Omni fusion operator factory.
     *
     * @param operatorFactories the operator factories
     * @param operatorTypes the operator types
     */
    public OmniFusionOperatorFactory(OmniOperatorFactory<?>[] operatorFactories, OperatorType[] operatorTypes) {
        this(operatorFactories, operatorTypes, new OperatorConfig());
    }

    private static native long createFusionOperatorFactory(long[] operatorFactories, int[] operatorTypes,
            String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        int operatorCount = context.operatorFactories.length;
        long[] nativeOperatorFactories = new long[operatorCount];
        int[] nativeOperatorTypes = new int[operatorCount];
        for (int i = 0; i < operatorCount; i++) {
            nativeOperatorFactories[i] = context.operatorFactories[i].getNativeOperatorFactory();
            nativeOperatorTypes[i] = context.operatorTypes[i].getValue();
        }
        return createFusionOperatorFactory(nativeOperatorFactories, nativeOperatorTypes,
                OperatorConfig.serialize(context.operatorConfig));
    }

    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final OmniOperatorFactory<?>[] operatorFactories;

        private final OperatorType[] operatorTypes;

        private final OperatorConfig operatorConfig;

        public FactoryContext(OmniOperatorFactory<?>[] operatorFactories, OperatorType[] operatorTypes,
                OperatorConfig operatorConfig) {
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null.");
            this.operatorTypes = requireNonNull(operatorTypes, "operatorTypes is null.");
            this.operatorConfig = operatorConfig;
            setNeedCache(false);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(operatorFactories), Arrays.hashCode(operatorTypes), operatorConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FactoryContext that = (FactoryContext) obj;
            return Arrays.equals(operatorFactories, that.operatorFactories)
                    && Arrays.equals(operatorTypes, that.operatorTypes) && operatorConfig.equals(that.operatorConfig);
        }
    }
}
