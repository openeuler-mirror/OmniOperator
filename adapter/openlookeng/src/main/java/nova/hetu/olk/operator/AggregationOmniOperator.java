/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.List;
import java.util.Optional;

/**
 * The type Aggregation omni operator.
 *
 * @since 20210630
 */
public class AggregationOmniOperator implements Operator {
    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private State state = State.NEEDS_INPUT;

    /**
     * Instantiates a new Aggregation omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public AggregationOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.omniOperator = requireNonNull(omniOperator, "omniOperator is null");
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
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
    }

    @Override
    public Page getOutput() {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        VecBatchToPageIterator pageIterator = new VecBatchToPageIterator(omniOperator.getOutput());
        if (pageIterator.hasNext()) {
            state = State.FINISHED;
            return pageIterator.next();
        }
        return null;
    }

    @Override
    public void finish() {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished() {
        return state == State.FINISHED;
    }

    @Override
    public void close() {
        omniOperator.close();
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

    /**
     * The type Aggregation omni operator factory.
     *
     * @since 20210630
     */
    public static class AggregationOmniOperatorFactory implements OperatorFactory {
        private static final int INVALID_MASK_CHANNEL = -1;

        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final DataType[] sourceTypes;
        private final Step step;
        private final FunctionType[] aggregatorTypes;
        private final int[] aggregationInputChannels;
        private final List<Optional<Integer>> maskChannels;
        private final DataType[] aggregationOutputTypes;
        private final OmniAggregationOperatorFactory omniFactory;

        /**
         * Instantiates a new Aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param aggregatorTypes the aggregations
         * @param aggregationInputChannels the accumulator factories
         * @param maskChannelList mask channel list for aggregators
         * @param aggregationOutputTypes
         * @param step the step
         */
        public AggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, DataType[] sourceTypes,
                FunctionType[] aggregatorTypes, int[] aggregationInputChannels, List<Optional<Integer>> maskChannelList,
                DataType[] aggregationOutputTypes, Step step) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.step = step;
            this.aggregatorTypes = aggregatorTypes;
            this.aggregationInputChannels = aggregationInputChannels;
            this.aggregationOutputTypes = aggregationOutputTypes;
            this.maskChannels = requireNonNull(maskChannelList, "mask channels is null");
            int[] maskChannelArray = new int[maskChannelList.size()];
            for (int i = 0; i < maskChannelList.size(); i++) {
                Optional<Integer> channel = maskChannelList.get(i);
                if (channel.isPresent()) {
                    maskChannelArray[i] = channel.get().intValue();
                } else {
                    maskChannelArray[i] = INVALID_MASK_CHANNEL;
                }
            }
            this.omniFactory = new OmniAggregationOperatorFactory(sourceTypes, aggregatorTypes,
                    aggregationInputChannels, maskChannelArray, aggregationOutputTypes, step.isInputRaw(),
                    step.isOutputPartial());
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            VecAllocator vecAllocator = VecAllocatorHelper
                    .getVecAllocatorFromTaskContext(driverContext.getPipelineContext().getTaskContext());
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    AggregationOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniFactory.createOperator(vecAllocator);
            return new AggregationOmniOperator(operatorContext, omniOperator);
        }

        @Override
        public void noMoreOperators() {
        }

        @Override
        public OperatorFactory duplicate() {
            return new AggregationOmniOperatorFactory(operatorId, planNodeId, sourceTypes, aggregatorTypes,
                    aggregationInputChannels, maskChannels, aggregationOutputTypes, step);
        }

        @Override
        public boolean isExtensionOperatorFactory() {
            return true;
        }
    }
}
