/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;
import static nova.hetu.olk.tool.OperatorUtils.getVecBatch;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;

import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The type Order by omni operator.
 *
 * @since 20210630
 */
public class OrderByOmniOperator implements Operator {
    /**
     * The type Order by omni operator factory.
     *
     * @since 20210630
     */
    public static class OrderByOmniOperatorFactory implements OperatorFactory {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final List<Type> sourceTypes;

        private final int[] outputChannels;

        private final int[] sortChannels;

        private final int[] sortAscendings;

        private final int[] sortNullFirsts;

        private final OmniSortOperatorFactory omniSortOperatorFactory;

        private boolean closed;

        /**
         * Create order by omni operator factory order by omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         * @return the order by omni operator factory
         */
        public static OrderByOmniOperatorFactory createOrderByOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
            List<? extends Type> sourceTypes, List<Integer> outputChannels, List<Integer> sortChannels,
            List<SortOrder> sortOrder) {
            VecType[] types = OperatorUtils.toVecTypes(sourceTypes);

            int sortColSize = sortChannels.size();
            int[] ascendings = new int[sortColSize];
            int[] nullFirsts = new int[sortColSize];
            for (int i = 0; i < sortColSize; i++) {
                SortOrder order = sortOrder.get(i);
                ascendings[i] = order.isAscending() ? 1 : 0;
                nullFirsts[i] = order.isNullsFirst() ? 1 : 0;
            }

            OmniSortOperatorFactory omniSortOperatorFactory = new OmniSortOperatorFactory(types,
                Ints.toArray(outputChannels), createExpressions(sortChannels), ascendings, nullFirsts);

            return new OrderByOmniOperatorFactory(operatorId, planNodeId, (List<Type>) sourceTypes,
                Ints.toArray(outputChannels), Ints.toArray(sortChannels), ascendings, nullFirsts,
                omniSortOperatorFactory);
        }

        /**
         * Instantiates a new Order by omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortAscendings the sort ascendings
         * @param sortNullFirsts the sort null firsts
         * @param omniSortOperatorFactory the omni sort operator factory
         */
        public OrderByOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
            int[] outputChannels, int[] sortChannels, int[] sortAscendings, int[] sortNullFirsts,
            OmniSortOperatorFactory omniSortOperatorFactory) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = outputChannels;
            this.sortChannels = sortChannels;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
            this.omniSortOperatorFactory = omniSortOperatorFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                OrderByOmniOperator.class.getSimpleName());
            OmniOperator omniSortOperator = omniSortOperatorFactory.createOperator();
            return new OrderByOmniOperator(operatorContext, sourceTypes, outputChannels, sortChannels, sortAscendings,
                sortNullFirsts, omniSortOperator);
        }

        /**
         * Create operator operator.
         *
         * @return the operator
         */
        public Operator createOperator() {
            // all this is prepared for a fake driverContext to avoid change the original pipeline
            Executor mockExecutor = MoreExecutors.directExecutor();
            ScheduledExecutorService mockScheduledExecutorService = newSingleThreadScheduledExecutor();
            TaskContext mockTaskContext = TestingTaskContext.createTaskContext(mockExecutor,
                mockScheduledExecutorService, TestingSession.testSessionBuilder().build());
            MemoryTrackingContext mockMemoryTrackingContext = new MemoryTrackingContext(
                newSimpleAggregatedMemoryContext(), newSimpleAggregatedMemoryContext(),
                newSimpleAggregatedMemoryContext());
            PipelineContext mockPipelineContext = new PipelineContext(1, mockTaskContext, mockExecutor,
                mockScheduledExecutorService, mockMemoryTrackingContext, false, false, false);
            DriverContext mockDriverContext = new DriverContext(mockPipelineContext, mockExecutor,
                mockScheduledExecutorService, mockMemoryTrackingContext, Lifespan.taskWide(), 0);
            OperatorContext mockOperatorContext = mockDriverContext.addOperatorContext(1,
                new PlanNodeId("Fake node for creating the OrderByOmniOperator"), "OrderByOmniOperator type");

            OmniOperator omniSortOperator = omniSortOperatorFactory.createOperator();
            return new OrderByOmniOperator(mockOperatorContext, sourceTypes, outputChannels, sortChannels,
                sortAscendings, sortNullFirsts, omniSortOperator);
        }

        @Override
        public void noMoreOperators() {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            return new OrderByOmniOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, sortChannels,
                sortAscendings, sortNullFirsts, omniSortOperatorFactory);
        }

        @Override
        public boolean isExtensionOperatorFactory() {
            return true;
        }

        @Override
        public List<Type> getSourceTypes() {
            return sourceTypes;
        }
    }

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
         * Finished state.
         */
        FINISHED
    }

    private final OperatorContext operatorContext;

    private final LocalMemoryContext revocableMemoryContext;

    private final LocalMemoryContext localUserMemoryContext;

    private final OmniOperator omniOperator;

    private final List<Type> sourceTypes;

    private final int[] outputChannels;

    private final List<VecBatch> inputVecBatchs = new ArrayList<>();

    private Iterator<Optional<Page>> sortedPages;

    private State state = State.NEEDS_INPUT;

    /**
     * Instantiates a new Order by omni operator.
     *
     * @param operatorContext the operator context
     * @param sourceTypes the source types
     * @param outputChannels the output channels
     * @param sortChannels the sort channels
     * @param sortAscendings the sort ascendings
     * @param sortNullFirsts the sort null firsts
     * @param omniOperator the omni operator
     */
    public OrderByOmniOperator(OperatorContext operatorContext, List<Type> sourceTypes, int[] outputChannels,
        int[] sortChannels, int[] sortAscendings, int[] sortNullFirsts, OmniOperator omniOperator) {
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.sourceTypes = sourceTypes;
        this.outputChannels = outputChannels;
        this.omniOperator = omniOperator;
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public boolean needsInput() {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        VecBatch vecBatch = getVecBatch(page, getClass().getSimpleName());
        omniOperator.addInput(vecBatch);
        inputVecBatchs.add(vecBatch);
    }

    @Override
    public Page getOutput() {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            inputVecBatchs.forEach(vecBatch -> vecBatch.releaseAllVectors());
            inputVecBatchs.forEach(vecBatch -> vecBatch.close());
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (!next.isPresent()) {
            return null;
        }
        Page nextPage = next.get();

        return nextPage;
    }

    @Override
    public void finish() {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // Convert revocable memory to user memory as sortedPages holds on to memory so we no longer can revoke.
            if (revocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = revocableMemoryContext.getBytes();
                revocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we don't
                    // have a proper way to atomically convert memory reservations
                    revocableMemoryContext.setBytes(currentRevocableBytes);
                    // spill since revocable memory could not be converted to user memory immediately
                    // TODO: this should be asynchronous
                }
            }
            sortedPages = transform(new VecBatchToPageIterator(omniOperator.getOutput()), Optional::of);
        }
    }

    @Override
    public boolean isFinished() {
        return state == State.FINISHED;
    }

    @Override
    public void close() throws Exception {
        omniOperator.close();
    }
}
