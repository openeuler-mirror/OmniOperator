/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Omni hash builder with expression operator factory.
 *
 * @since 2021-10-16
 */
public class OmniHashBuilderWithExprOperatorFactory
        extends OmniOperatorFactory<OmniHashBuilderWithExprOperatorFactory.FactoryContext> {
    /**
     * The global lock is used for sharing hash builder operator and factory concurrently.
     */
    public static final Lock gLock = new ReentrantLock();

    /**
     * The hashmap cached the shared hash builder operator, the key is the build plan node id.
     */
    private static Map<Integer, OmniOperator> operatorCache = new HashMap<>();

    /**
     * The hashmap cached the shared hash builder operator factory, the key is the build plan node id.
     */
    private static Map<Integer, OmniHashBuilderWithExprOperatorFactory> factoryCache = new HashMap<>();

    /**
     * The hashmap stores the num of lookup which is in-use shared hash builder operator
     * the key is the build plan node id.
     */
    private static Map<Integer, AtomicInteger> ref = new HashMap<>();

    /**
     * Instantiates a new Omni hash builder with expression operator factory.
     *
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param filterExpression the filter expression in join
     * @param operatorCount the operator count
     * @param operatorConfig the operator config
     */
    public OmniHashBuilderWithExprOperatorFactory(DataType[] buildTypes, String[] buildHashKeys,
            Optional<String> filterExpression, int operatorCount, OperatorConfig operatorConfig) {
        super(new FactoryContext(buildTypes, buildHashKeys, filterExpression, operatorCount, operatorConfig));
    }

    /**
     * Instantiates a new Omni hash builder with expression operator factory with
     * default operator config.
     *
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param filterExpression the filter expression in join
     * @param operatorCount the operator count
     */
    public OmniHashBuilderWithExprOperatorFactory(DataType[] buildTypes, String[] buildHashKeys,
            Optional<String> filterExpression, int operatorCount) {
        this(buildTypes, buildHashKeys, filterExpression, operatorCount, new OperatorConfig());
    }

    private static native long createHashBuilderWithExprOperatorFactory(String buildTypes, String[] buildHashKeys,
            String filterExpression, int operatorCount, String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createHashBuilderWithExprOperatorFactory(DataTypeSerializer.serialize(context.buildTypes),
                context.buildHashKeys, context.filterExpression, context.operatorCount,
                OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * save a new shared hash builder operator and factory into cache
     *
     * @param builderNodeId the build plan node id
     * @param factory the shared hash builder operator factory
     * @param operator the shared hash builder operator
     */
    public static void saveHashBuilderOperatorAndFactory(Integer builderNodeId,
                                                         OmniHashBuilderWithExprOperatorFactory factory,
                                                         OmniOperator operator) {
        operatorCache.put(builderNodeId, operator);
        factoryCache.put(builderNodeId, factory);
    }

    /**
     * try get a shared hash builder factory form cache
     *
     * @param builderNodeId the build plan node id
     * @return the shared hash builder operator factory
     */
    public static OmniHashBuilderWithExprOperatorFactory getHashBuilderOperatorFactory(Integer builderNodeId) {
        ref.computeIfAbsent(builderNodeId, key -> new AtomicInteger(0));
        ref.get(builderNodeId).incrementAndGet();
        return factoryCache.get(builderNodeId);
    }

    /**
     * try close and remove a shared hash builder factory form cache
     *
     * @param builderNodeId the build plan node id
     */
    public static void dereferenceHashBuilderOperatorAndFactory(Integer builderNodeId) {
        if (ref.get(builderNodeId).decrementAndGet() == 0) {
            ref.remove(builderNodeId);
            operatorCache.get(builderNodeId).close();
            operatorCache.remove(builderNodeId);
            factoryCache.get(builderNodeId).close();
            factoryCache.remove(builderNodeId);
        }
    }

    /**
     * The Factory context.
     *
     * @since 2021-10-16
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final DataType[] buildTypes;

        private final String[] buildHashKeys;

        private final String filterExpression;

        private final int operatorCount;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param buildTypes the build types
         * @param buildHashKeys the build hash keys
         * @param filterExpression the join filter expression
         * @param operatorCount the operator count
         * @param operatorConfig the operator config
         */
        public FactoryContext(DataType[] buildTypes, String[] buildHashKeys, Optional<String> filterExpression,
                int operatorCount, OperatorConfig operatorConfig) {
            this.buildTypes = requireNonNull(buildTypes, "buildTypes");
            this.buildHashKeys = requireNonNull(buildHashKeys, "buildHashKeys");
            this.filterExpression = filterExpression.isPresent() ? filterExpression.get() : "";
            this.operatorCount = operatorCount;
            this.operatorConfig = operatorConfig;
            setNeedCache(false);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(buildTypes), Arrays.hashCode(buildHashKeys), filterExpression,
                    operatorCount, operatorConfig);
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
            return Arrays.equals(buildTypes, that.buildTypes) && Arrays.equals(buildHashKeys, that.buildHashKeys)
                    && filterExpression.equals(that.filterExpression) && operatorCount == that.operatorCount
                    && operatorConfig.equals(that.operatorConfig);
        }
    }
}
