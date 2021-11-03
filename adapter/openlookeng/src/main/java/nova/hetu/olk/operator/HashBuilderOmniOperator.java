/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.HashCollisionsCounter;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupSource;
import io.prestosql.operator.LookupSourceSupplier;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The type Hash builder omni operator.
 *
 * @since 20210630
 */
@ThreadSafe
public class HashBuilderOmniOperator implements Operator {
    /**
     * The type Hash builder omni operator factory.
     *
     * @since 20210630
     */
    public static class HashBuilderOmniOperatorFactory implements OperatorFactory {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;

        private final List<Integer> outputChannels;

        private final List<Integer> hashChannels;

        private final OptionalInt preComputedHashChannel;

        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

        private final Optional<Integer> sortChannel;

        private final List<JoinFilterFunctionFactory> searchFunctionFactories;

        private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();

        private final OmniHashBuilderOperatorFactory omniHashBuilderOperatorFactory;

        private final List<Type> buildTypes;

        private boolean closed;

        /**
         * Instantiates a new Hash builder omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param buildTypes the build types
         * @param outputChannels the output channels
         * @param hashChannels the hash channels
         * @param preComputedHashChannel the pre computed hash channel
         * @param filterFunctionFactory the filter function factory
         * @param sortChannel the sort channel
         * @param searchFunctionFactories the search function factories
         * @param operatorCount the operator count
         */
        public HashBuilderOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, List<Type> buildTypes,
            List<Integer> outputChannels, List<Integer> hashChannels, OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory, Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories, int operatorCount) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(),
                "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager,
                "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.buildTypes= ImmutableList.copyOf(requireNonNull(buildTypes, "sourceTypes is null"));

            VecType[] types = OperatorUtils.toVecTypes(buildTypes);
            this.omniHashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(types,
                createExpressions(hashChannels), operatorCount);
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.getVecAllocatorFromTaskContext(driverContext.getPipelineContext().getTaskContext());
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                HashBuilderOmniOperator.class.getSimpleName());

            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(
                driverContext.getLifespan());
            int partitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
            verify(partitionIndex < lookupSourceFactory.partitions());
            OmniOperator omniOperator = omniHashBuilderOperatorFactory.createOperator(vecAllocator);
            return new HashBuilderOmniOperator(operatorContext, lookupSourceFactory, partitionIndex, outputChannels,
                hashChannels, preComputedHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories,
                omniOperator);
        }

        @Override
        public void noMoreOperators() {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }

        @Override
        public boolean isExtensionOperatorFactory() {
            return true;
        }

        /**
         * Gets output channels.
         *
         * @return the output channels
         */
        public List<Integer> getOutputChannels() {
            return outputChannels;
        }

        /**
         * Gets omni hash builder operator factory.
         *
         * @return the omni hash builder operator factory
         */
        public OmniHashBuilderOperatorFactory getOmniHashBuilderOperatorFactory() {
            return omniHashBuilderOperatorFactory;
        }

        private int getAndIncrementPartitionIndex(Lifespan lifespan) {
            return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
        }

        @Override
        public List<Type> getSourceTypes() {
            return buildTypes;
        }
    }

    /**
     * The enum State.
     */
    @VisibleForTesting
    public enum State {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private final OperatorContext operatorContext;

    private final LocalMemoryContext localUserMemoryContext;

    private final LocalMemoryContext localRevocableMemoryContext;

    private final PartitionedLookupSourceFactory lookupSourceFactory;

    private final ListenableFuture<?> lookupSourceFactoryDestroyed;

    private final int partitionIndex;

    private final List<Integer> outputChannels;

    private final List<Integer> hashChannels;

    private final OptionalInt preComputedHashChannel;

    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    private final Optional<Integer> sortChannel;

    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;

    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();

    private final OmniOperator omniOperator;

    private final List<VecBatch> inputVecBatches = new ArrayList<>();

    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();

    /**
     * Instantiates a new Hash builder omni operator.
     *
     * @param operatorContext the operator context
     * @param lookupSourceFactory the lookup source factory
     * @param partitionIndex the partition index
     * @param outputChannels the output channels
     * @param hashChannels the hash channels
     * @param preComputedHashChannel the pre computed hash channel
     * @param filterFunctionFactory the filter function factory
     * @param sortChannel the sort channel
     * @param searchFunctionFactories the search function factories
     * @param omniOperator the omni operator
     */
    public HashBuilderOmniOperator(OperatorContext operatorContext, PartitionedLookupSourceFactory lookupSourceFactory,
        int partitionIndex, List<Integer> outputChannels, List<Integer> hashChannels,
        OptionalInt preComputedHashChannel, Optional<JoinFilterFunctionFactory> filterFunctionFactory,
        Optional<Integer> sortChannel, List<JoinFilterFunctionFactory> searchFunctionFactories,
        OmniOperator omniOperator) {
        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.omniOperator = omniOperator;
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    /**
     * Gets state.
     *
     * @return the state
     */
    @VisibleForTesting
    public State getState() {
        return state;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(
                    () -> new IllegalStateException("Lookup source built, but disposal future not set"));

            case CLOSED:
                return NOT_BLOCKED;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput() {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT);
        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page) {
        requireNonNull(page, "page is null");

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);

        operatorContext.recordOutput(page.getSizeInBytes(), positionCount);

        inputVecBatches.add(vecBatch);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        if (state == State.CONSUMING_INPUT) {
            finishMemoryRevoke = Optional.of(() -> {
                localRevocableMemoryContext.setBytes(0);
            });
        }
        if (state == State.LOOKUP_SOURCE_BUILT) {
            finishMemoryRevoke = Optional.of(() -> {
                lookupSourceNotNeeded = Optional.empty();
                localRevocableMemoryContext.setBytes(0);
                lookupSourceSupplier = null;
            });
        }
        if (operatorContext.getReservedRevocableBytes() == 0) {
            // Probably stale revoking request
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateFuture(null);
        }

        throw new IllegalStateException(
            format("State %s can not have revocable memory, but has %s revocable bytes", state,
                operatorContext.getReservedRevocableBytes()));
    }

    @Override
    public void finishMemoryRevoke() {
        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public Page getOutput() {
        return null;
    }

    @Override
    public void finish() {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput() {
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        omniOperator.getOutput();
        LookupSourceSupplier partition = buildLookupSource();
        localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested() {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        localRevocableMemoryContext.setBytes(0);
        lookupSourceSupplier = null;
        close();
    }

    private LookupSourceSupplier buildLookupSource() {
        LookupSourceSupplier partition = new EmptyJoinHashSupplier();
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    @Override
    public boolean isFinished() {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    @Override
    public void close() {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        omniOperator.close();
        inputVecBatches.forEach(vecBatch -> vecBatch.releaseAllVectors());
        inputVecBatches.forEach(vecBatch -> vecBatch.close());
        lookupSourceSupplier = null;
        state = State.CLOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class EmptyJoinHashSupplier implements LookupSourceSupplier {
        @Override
        public long getHashCollisions() {
            return 0;
        }

        @Override
        public double getExpectedHashCollisions() {
            return 0;
        }

        @Override
        public long checksum() {
            return 0;
        }

        @Override
        public LookupSource get() {
            return new EmptyJoinHash();
        }
    }

    private class EmptyJoinHash implements LookupSource {
        @Override
        public int getChannelCount() {
            return 0;
        }

        @Override
        public long getInMemorySizeInBytes() {
            return 0;
        }

        @Override
        public long getJoinPositionCount() {
            return 0;
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition) {
            return 0;
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash) {
            return 0;
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage) {
            return 0;
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage) {
            return 0;
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset) {
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage) {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public void close() {
        }
    }
}
