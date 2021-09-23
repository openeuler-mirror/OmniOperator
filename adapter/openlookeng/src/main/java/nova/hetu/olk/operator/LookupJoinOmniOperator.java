/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.operator.LookupJoinOperators.JoinType.INNER;
import static io.prestosql.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_FULL;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_LEFT;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_RIGHT;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.EmptyLookupSource;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.JoinOperatorFactory;
import io.prestosql.operator.JoinStatisticsCounter;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.LookupOuterOperator;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.LookupSourceProvider;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.StaticLookupSourceProvider;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

/**
 * The type Lookup join omni operator.
 *
 * @since 20210630
 */
public class LookupJoinOmniOperator implements Operator {
    private final OperatorContext operatorContext;

    private final List<Type> probeTypes;

    private final Runnable afterClose;

    private final OptionalInt lookupJoinsCount;

    private final LookupSourceFactory lookupSourceFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;

    private final OmniOperator omniOperator;

    private LookupSourceProvider lookupSourceProvider;

    private Iterator<Page> result;

    private Page outputPage;

    private boolean closed;

    private State state = State.NEEDS_INPUT;

    private enum State {
        /**
         * Needs input state.
         */
        NEEDS_INPUT,
        /**
         * Has output state.
         */
        HAS_OUTPUT,
        /**
         * Finishing state.
         */
        FINISHING,
        /**
         * Finished state.
         */
        FINISHED
    }

    /**
     * Instantiates a new Lookup join omni operator.
     *
     * @param operatorContext the operator context
     * @param probeTypes the probe types
     * @param joinType the join type
     * @param lookupSourceFactory the lookup source factory
     * @param afterClose the after close
     * @param lookupJoinsCount the lookup joins count
     * @param omniOperator the omni operator
     */
    public LookupJoinOmniOperator(OperatorContext operatorContext, List<Type> probeTypes, JoinType joinType,
        LookupSourceFactory lookupSourceFactory, Runnable afterClose, OptionalInt lookupJoinsCount,
        OmniOperator omniOperator) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");

        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.omniOperator = omniOperator;
    }

    /**
     * Inner join operator factory.
     *
     * @param operatorId the operator id
     * @param planNodeId the plan node id
     * @param lookupSourceFactory the lookup source factory
     * @param probeTypes the probe types
     * @param probeJoinChannel the probe join channel
     * @param probeHashChannel the probe hash channel
     * @param probeOutputChannels the probe output channels
     * @param totalOperatorsCount the total operators count
     * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
     * @return the operator factory
     */
    public static OperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId,
        JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes,
        List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels,
        OptionalInt totalOperatorsCount,
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        return createOmniJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel,
            probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.INNER,
            totalOperatorsCount, hashBuilderOmniOperatorFactory);
    }

    /**
     * Probe outer join operator factory.
     *
     * @param operatorId the operator id
     * @param planNodeId the plan node id
     * @param lookupSourceFactory the lookup source factory
     * @param probeTypes the probe types
     * @param probeJoinChannel the probe join channel
     * @param probeHashChannel the probe hash channel
     * @param probeOutputChannels the probe output channels
     * @param totalOperatorsCount the total operators count
     * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
     * @return the operator factory
     */
    public static OperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId,
        JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes,
        List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels,
        OptionalInt totalOperatorsCount,
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        return createOmniJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel,
            probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.PROBE_OUTER,
            totalOperatorsCount, hashBuilderOmniOperatorFactory);
    }

    /**
     * Lookup outer join operator factory.
     *
     * @param operatorId the operator id
     * @param planNodeId the plan node id
     * @param lookupSourceFactory the lookup source factory
     * @param probeTypes the probe types
     * @param probeJoinChannel the probe join channel
     * @param probeHashChannel the probe hash channel
     * @param probeOutputChannels the probe output channels
     * @param totalOperatorsCount the total operators count
     * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
     * @return the operator factory
     */
    public static OperatorFactory lookupOuterJoin(int operatorId, PlanNodeId planNodeId,
        JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes,
        List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels,
        OptionalInt totalOperatorsCount,
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        return createOmniJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel,
            probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.LOOKUP_OUTER,
            totalOperatorsCount, hashBuilderOmniOperatorFactory);
    }

    /**
     * Full outer join operator factory.
     *
     * @param operatorId the operator id
     * @param planNodeId the plan node id
     * @param lookupSourceFactory the lookup source factory
     * @param probeTypes the probe types
     * @param probeJoinChannel the probe join channel
     * @param probeHashChannel the probe hash channel
     * @param probeOutputChannels the probe output channels
     * @param totalOperatorsCount the total operators count
     * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
     * @return the operator factory
     */
    public static OperatorFactory fullOuterJoin(int operatorId, PlanNodeId planNodeId,
        JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes,
        List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels,
        OptionalInt totalOperatorsCount,
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        return createOmniJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel,
            probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.FULL_OUTER,
            totalOperatorsCount, hashBuilderOmniOperatorFactory);
    }

    private static List<Integer> rangeList(int endExclusive) {
        return IntStream.range(0, endExclusive).boxed().collect(toImmutableList());
    }

    private static OperatorFactory createOmniJoinOperatorFactory(int operatorId, PlanNodeId planNodeId,
        JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager, List<Type> probeTypes,
        List<Integer> probeJoinChannel, OptionalInt probeHashChannel, List<Integer> probeOutputChannels,
        JoinType joinType, OptionalInt totalOperatorsCount,
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
            .map(probeTypes::get)
            .collect(toImmutableList());

        return new LookupJoinOmniOperatorFactory(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes,
            probeOutputChannels, probeOutputChannelTypes, joinType, totalOperatorsCount, probeJoinChannel,
            probeHashChannel, hashBuilderOmniOperatorFactory);
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public void finish() {
        state = State.FINISHING;
    }

    @Override
    public boolean isFinished() {
        boolean finished = state == State.FINISHED;

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput() {
        return state == State.NEEDS_INPUT && lookupSourceProviderFuture.isDone();
    }

    @Override
    public void addInput(Page page) {
        requireNonNull(page, "page is null");

        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }
        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, getClass().getSimpleName());
        omniOperator.addInput(vecBatch);
        result = new VecBatchToPageIterator(omniOperator.getOutput());
        if (!result.hasNext()) {
            return;
        }
        state = State.HAS_OUTPUT;
        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    private boolean tryFetchLookupSourceProvider() {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(
                lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    @Override
    public Page getOutput() {
        if (state == State.NEEDS_INPUT || state == State.FINISHED) {
            return null;
        }

        // TODO introduce explicit state (enum), like in HBO
        if (!tryFetchLookupSourceProvider()) {
            if (!(state == State.FINISHING)) {
                return null;
            }

            verify(state == State.FINISHING);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        // it has gotted all output
        if (state == State.FINISHING && result == null) {
            state = State.FINISHED;
            return null;
        }

        if (!result.hasNext()) {
            result = null;
            if (state == State.FINISHING) {
                state = State.FINISHED;
            } else if (state == State.HAS_OUTPUT) {
                state = State.NEEDS_INPUT;
            }
            return null;
        }

        outputPage = result.next();
        if (outputPage != null) {
            Page output = outputPage;
            outputPage = null;
            return output;
        }

        return null;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        omniOperator.close();

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The type Lookup join omni operator factory.
     *
     * @since 20210630
     */
    public static class LookupJoinOmniOperatorFactory implements JoinOperatorFactory {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final List<Type> probeTypes;

        private final JoinType joinType;

        private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;

        private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;

        private final OptionalInt totalOperatorsCount;

        private final OmniLookupJoinOperatorFactory omniLookupJoinOperatorFactory;

        private boolean closed;

        /**
         * Instantiates a new Lookup join omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param probeTypes the probe types
         * @param probeOutputChannels the probe output channels
         * @param probeOutputChannelTypes the probe output channel types
         * @param joinType the join type
         * @param totalOperatorsCount the total operators count
         * @param probeJoinChannel the probe join channel
         * @param probeHashChannel the probe hash channel
         * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
         */
        public LookupJoinOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager, List<Type> probeTypes,
            List<Integer> probeOutputChannels, List<Type> probeOutputChannelTypes, JoinType joinType,
            OptionalInt totalOperatorsCount, List<Integer> probeJoinChannel, OptionalInt probeHashChannel,
            HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
            this.joinType = requireNonNull(joinType, "joinType is null");

            this.joinBridgeManager = lookupSourceFactoryManager;
            joinBridgeManager.incrementProbeFactoryCount();

            List<Integer> buildOutputChannels = hashBuilderOmniOperatorFactory.getOutputChannels();
            List<Type> buildOutputTypes = lookupSourceFactoryManager.getBuildOutputTypes();

            if (joinType == INNER || joinType == PROBE_OUTER) {
                this.outerOperatorFactoryResult = Optional.empty();
            } else {
                this.outerOperatorFactoryResult = Optional.of(new OuterOperatorFactoryResult(
                    new LookupOuterOperator.LookupOuterOperatorFactory(operatorId, planNodeId, probeOutputChannelTypes,
                        buildOutputTypes, lookupSourceFactoryManager),
                    lookupSourceFactoryManager.getBuildExecutionStrategy()));
            }
            this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");

            requireNonNull(probeHashChannel, "probeHashChannel is null");

            VecType[] types = OperatorUtils.toVecTypes(probeTypes);
            VecType[] buildOutputVecTypes = OperatorUtils.toVecTypes(buildOutputTypes);

            this.omniLookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(types,
                Ints.toArray(probeOutputChannels), createExpressions(probeJoinChannel),
                Ints.toArray(buildOutputChannels), buildOutputVecTypes, getOmniJoinType(joinType),
                hashBuilderOmniOperatorFactory.getOmniHashBuilderOperatorFactory());
        }

        private nova.hetu.omniruntime.constants.JoinType getOmniJoinType(JoinType joinType) {
            nova.hetu.omniruntime.constants.JoinType omniJoinType;
            switch (joinType) {
                case INNER:
                    omniJoinType = OMNI_JOIN_TYPE_INNER;
                    break;
                case PROBE_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_LEFT;
                    break;
                case LOOKUP_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_RIGHT;
                    break;
                case FULL_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_FULL;
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported join type : " + joinType);
            }
            return omniJoinType;
        }

        private LookupJoinOmniOperatorFactory(LookupJoinOmniOperatorFactory other) {
            requireNonNull(other, "other is null");
            checkArgument(!other.closed, "cannot duplicated closed OperatorFactory");

            operatorId = other.operatorId;
            planNodeId = other.planNodeId;
            probeTypes = other.probeTypes;
            joinType = other.joinType;
            joinBridgeManager = other.joinBridgeManager;
            outerOperatorFactoryResult = other.outerOperatorFactoryResult;
            totalOperatorsCount = other.totalOperatorsCount;

            closed = false;
            joinBridgeManager.incrementProbeFactoryCount();
            omniLookupJoinOperatorFactory = other.omniLookupJoinOperatorFactory;
        }

        /**
         * Gets operator id.
         *
         * @return the operator id
         */
        public int getOperatorId() {
            return operatorId;
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.getVecAllocatorFromTaskContext(driverContext.getPipelineContext().getTaskContext());

            LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                LookupJoinOmniOperator.class.getSimpleName());

            lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

            joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
            OmniOperator omniOperator = omniLookupJoinOperatorFactory.createOperator(vecAllocator);
            return new LookupJoinOmniOperator(operatorContext, probeTypes, joinType, lookupSourceFactory,
                () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()), totalOperatorsCount,
                omniOperator);
        }

        @Override
        public void noMoreOperators() {
            checkState(!closed);
            closed = true;
            joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan) {
            joinBridgeManager.probeOperatorFactoryClosed(lifespan);
        }

        @Override
        public OperatorFactory duplicate() {
            return new LookupJoinOmniOperatorFactory(this);
        }

        @Override
        public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory() {
            return outerOperatorFactoryResult;
        }

        @Override
        public boolean isExtensionOperatorFactory() {
            return true;
        }

        @Override
        public List<Type> getSourceTypes() {
            return probeTypes;
        }
    }
}
