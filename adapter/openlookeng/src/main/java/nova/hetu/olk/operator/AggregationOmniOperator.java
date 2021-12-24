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
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniAggregationOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.List;

/**
 * The type Aggregation omni operator.
 *
 * @since 20210630
 */
public class AggregationOmniOperator implements Operator {
    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private final int[] aggregationChannels;

    private State state = State.NEEDS_INPUT;

    /**
     * Instantiates a new Aggregation omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     * @param aggregationChannels the aggregation channels
     */
    public AggregationOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator,
        int[] aggregationChannels) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.omniOperator = requireNonNull(omniOperator, "omniOperator is null");
        this.aggregationChannels = aggregationChannels;
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
        vecBatch.releaseAllVectors();
        vecBatch.close();
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
        private final OmniAggregationOperatorFactory omniFactory;

        private final Step step;
        private final ImmutableList<Aggregation> aggregations;
        private final ImmutableList<AccumulatorFactory> accumulatorFactories;

        /**
         * The Operator id.
         */
        int operatorId;

        /**
         * The Plan node id.
         */
        PlanNodeId planNodeId;

        /**
         * The Aggregation channels.
         */
        int[] aggregationChannels;

        private List<Type> sourceTypes;

        /**
         * Instantiates a new Aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param types
         * @param aggregations the aggregations
         * @param accumulatorFactories the accumulator factories
         * @param step the step
         */
        public AggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> types,
            ImmutableList<Aggregation> aggregations, ImmutableList<AccumulatorFactory> accumulatorFactories,
            Step step) {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.step = step;
            this.aggregations = aggregations;
            this.accumulatorFactories = accumulatorFactories;

            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));
            int aggregationSize = aggregations.size();

            this.aggregationChannels = new int[aggregationSize];
            VecType[] aggregationTypes = new VecType[aggregationSize];
            AggType[] aggregationFuncTypes = new AggType[aggregationSize];
            VecType[] aggReturnTypes = new VecType[aggregationSize];

            for (int i = 0; i < aggregationSize; i++) {
                Signature signature = aggregations.get(i).getSignature();
                aggregationChannels[i] = accumulatorFactories.get(i).getInputChannels().get(0);
                aggregationTypes[i] = OperatorUtils.toVecType(signature.getArgumentTypes().get(0));
                aggReturnTypes[i] = OperatorUtils.toVecType(signature.getReturnType());
                switch (signature.getName()) {
                    case "sum":
                        aggregationFuncTypes[i] = AggType.OMNI_AGGREGATION_TYPE_SUM;
                        break;
                    case "avg":
                        aggregationFuncTypes[i] = AggType.OMNI_AGGREGATION_TYPE_AVG;
                        break;
                    case "count":
                        aggregationFuncTypes[i] = AggType.OMNI_AGGREGATION_TYPE_COUNT;
                        break;
                    case "max":
                        aggregationFuncTypes[i] = AggType.OMNI_AGGREGATION_TYPE_MAX;
                        break;
                    case "min":
                        aggregationFuncTypes[i] = AggType.OMNI_AGGREGATION_TYPE_MIN;
                        break;
                    default:
                        throw new UnsupportedOperationException(
                            "unsupported Aggregator type by OmniRuntime: " + signature.getName());
                }
            }
            this.omniFactory = new OmniAggregationOperatorFactory(aggregationTypes, aggregationFuncTypes,
                aggReturnTypes, this.step.isInputRaw(), this.step.isOutputPartial());
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            VecAllocator vecAllocator = VecAllocatorHelper.getVecAllocatorFromTaskContext(driverContext.getPipelineContext().getTaskContext());
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                AggregationOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniFactory.createOperator(vecAllocator);
            return new AggregationOmniOperator(operatorContext, omniOperator, aggregationChannels);
        }

        @Override
        public void noMoreOperators() {
        }

        @Override
        public OperatorFactory duplicate() {
            return new AggregationOmniOperatorFactory(operatorId, planNodeId, sourceTypes, aggregations, accumulatorFactories, step);
        }

        @Override
        public boolean isExtensionOperatorFactory() {
            return true;
        }
    }
}
