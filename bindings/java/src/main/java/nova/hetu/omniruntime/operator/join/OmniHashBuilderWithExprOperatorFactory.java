/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.join;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.constants.JoinType;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
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
     * The global lock is used for sharing hash builder operator and factory
     * concurrently.
     */
    public static final Lock gLock = new ReentrantLock();

    /**
     * The hashmap cached the shared hash builder operator, the key is the build
     * plan node id.
     */
    private static Map<Integer, OmniOperator> operatorCache = new HashMap<>();

    /**
     * The hashmap cached the shared hash builder operator factory, the key is the
     * build plan node id.
     */
    private static Map<Integer, OmniHashBuilderWithExprOperatorFactory> factoryCache = new HashMap<>();

    /**
     * The hashmap stores the num of lookup which is in-use shared hash builder
     * operator
     * the key is the build plan node id.
     */
    private static Map<Integer, AtomicInteger> ref = new HashMap<>();

    /**
     * The hashmap stores the partition id each task.
     * the key is the build plan node id.
     */
    private static Map<Integer, Integer> partitionCache = new HashMap<>();

    /**
     * The signal is used for multi thread communication
     */
    private final Condition signal = gLock.newCondition();

    /**
     * Instantiates a new Omni hash builder with expression operator factory.
     *
     * @param joinType the join type
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param operatorCount the operator count
     * @param operatorConfig the operator config
     */
    public OmniHashBuilderWithExprOperatorFactory(JoinType joinType, DataType[] buildTypes, String[] buildHashKeys,
            int operatorCount, OperatorConfig operatorConfig) {
        super(new FactoryContext(joinType, buildTypes, buildHashKeys, operatorCount, operatorConfig));
    }

    /**
     * Instantiates a new Omni hash builder with expression operator factory with
     * default operator config.
     *
     * @param joinType the join type
     * @param buildTypes the build input types
     * @param buildHashKeys the build hash keys
     * @param operatorCount the operator count
     */
    public OmniHashBuilderWithExprOperatorFactory(JoinType joinType, DataType[] buildTypes, String[] buildHashKeys,
            int operatorCount) {
        this(joinType, buildTypes, buildHashKeys, operatorCount, new OperatorConfig());
    }

    private static native long createHashBuilderWithExprOperatorFactory(int joinType, String buildTypes,
            String[] buildHashKeys, int operatorCount, String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createHashBuilderWithExprOperatorFactory(context.joinType.getValue(),
                DataTypeSerializer.serialize(context.buildTypes), context.buildHashKeys, context.operatorCount,
                OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * Attempts to clear the operator and factory objects. If the value of refCount is not 0, the thread waits.
     *
     * @param builderNodeId the build plan node id
     * @throws InterruptedException thread await
     */
    public void tryCloseOperatorAndFactory(Integer builderNodeId) throws InterruptedException {
        gLock.lock();
        int refCount = OmniHashBuilderWithExprOperatorFactory.dereferenceHashBuilderOperatorAndFactory(builderNodeId);
        try {
            while (refCount != 0) {
                signal.await();
                refCount = ref.get(builderNodeId).get();
            }
            OmniHashBuilderWithExprOperatorFactory.removeHashBuilderOperatorAndFactory(builderNodeId);
        } finally {
            gLock.unlock();
        }
    }

    /**
     * Attempts to reduce the recCount of the operator and factory objects.
     * If the value is 0, wake up the suspended thread.
     *
     * @param builderNodeId the build plan node id
     */
    public void tryDereferenceOperatorAndFactory(Integer builderNodeId) {
        gLock.lock();
        try {
            int refCount = OmniHashBuilderWithExprOperatorFactory.dereferenceHashBuilderOperatorAndFactory(
                    builderNodeId);
            if (refCount == 0) {
                // wakes up suspends task thread when the ref count is 0.
                signal.signal();
            }
        } finally {
            gLock.unlock();
        }
    }

    /**
     * save a new shared hash builder operator and factory into cache
     *
     * @param builderNodeId the build plan node id
     * @param partitionId the partition id where the shared object created
     * @param factory the shared hash builder operator factory
     * @param operator the shared hash builder operator
     */
    public static void saveHashBuilderOperatorAndFactory(Integer builderNodeId, Integer partitionId,
            OmniHashBuilderWithExprOperatorFactory factory, OmniOperator operator) {
        operatorCache.put(builderNodeId, operator);
        factoryCache.put(builderNodeId, factory);
        partitionCache.put(builderNodeId, partitionId);
    }

    /**
     * get a shared hash builder factory from cache
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
     * get the partition index from cache
     *
     * @param builderNodeId the build plan node id
     * @return the partition index of the current build plan node id
     */
    public static Integer getPartitionId(Integer builderNodeId) {
        return partitionCache.get(builderNodeId);
    }

    /**
     * reduce the refCount of the operator and factory objects.
     *
     * @param builderNodeId the build plan node id
     * @return the refCount of the current build plan node id
     */
    public static Integer dereferenceHashBuilderOperatorAndFactory(Integer builderNodeId) {
        return ref.get(builderNodeId).decrementAndGet();
    }

    /**
     * close and remove a shared hash builder factory from cache
     *
     * @param builderNodeId the build plan node id
     */
    public static void removeHashBuilderOperatorAndFactory(Integer builderNodeId) {
        if (ref.get(builderNodeId).get() == 0) {
            partitionCache.remove(builderNodeId);
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
        private final JoinType joinType;

        private final DataType[] buildTypes;

        private final String[] buildHashKeys;

        private final int operatorCount;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param joinType the join type
         * @param buildTypes the build types
         * @param buildHashKeys the build hash keys
         * @param operatorCount the operator count
         * @param operatorConfig the operator config
         */
        public FactoryContext(JoinType joinType, DataType[] buildTypes, String[] buildHashKeys, int operatorCount,
                OperatorConfig operatorConfig) {
            this.joinType = requireNonNull(joinType, "joinType");
            this.buildTypes = requireNonNull(buildTypes, "buildTypes");
            this.buildHashKeys = requireNonNull(buildHashKeys, "buildHashKeys");
            this.operatorCount = operatorCount;
            this.operatorConfig = operatorConfig;
            setNeedCache(false);
        }

        @Override
        public int hashCode() {
            return Objects.hash(joinType, Arrays.hashCode(buildTypes), Arrays.hashCode(buildHashKeys), operatorCount,
                    operatorConfig);
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
            return joinType.equals(that.joinType) && Arrays.equals(buildTypes, that.buildTypes)
                    && Arrays.equals(buildHashKeys, that.buildHashKeys) && operatorCount == that.operatorCount
                    && operatorConfig.equals(that.operatorConfig);
        }
    }
}
