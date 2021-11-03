/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationOperatorFactory;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The type Hash aggregation omni operator.
 *
 * @since 20210630
 */
public class HashAggregationOmniOperator implements Operator {
    private static final Logger log = Logger.get(HashAggregationOmniOperator.class);

    private final OmniOperator omniOperator;

    private final OperatorContext operatorContext;

    /**
     * The Pages.
     */
    Iterator<Page> pages;

    private boolean finishing;

    private boolean finished;

    /**
     * Instantiates a new Hash aggregation omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public HashAggregationOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null.");
        this.omniOperator = requireNonNull(omniOperator, "omniOperator is null.");
    }

    @Override
    public OperatorContext getOperatorContext() {
        return this.operatorContext;
    }

    @Override
    public void finish() {
        finishing = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }


    @Override
    public void close() {
        omniOperator.close();
    }

    @Override
    public boolean needsInput() {
        return !finishing;
    }

    @Override
    public void addInput(Page page) {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
        vecBatch.releaseAllVectors();
        vecBatch.close();
    }

    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }
        if (finishing) {
            if (pages == null) {
                pages = new VecBatchToPageIterator(omniOperator.getOutput());
            } else {
                if (pages.hasNext()) {
                    return pages.next();
                } else {
                    finished = true;
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        return null;
    }

    @Override
    public void finishMemoryRevoke() {
    }

    /**
     * The type Hash aggregation omni operator factory.
     *
     * @since 20210630
     */
    public static class HashAggregationOmniOperatorFactory implements OperatorFactory {
        private final OmniHashAggregationOperatorFactory omniFactory;

        private final Step step;

        private List<Type> sourceTypes;

        /**
         * The Operator id.
         */
        int operatorId;

        /**
         * The Plan node id.
         */
        PlanNodeId planNodeId;

        private int[] groupByInputChannels;

        private VecType[] groupByInputTypes;

        private int[] aggregationInputChannels;

        private VecType[] aggregationInputTypes;

        private AggType[] aggregatorTypes;

        private VecType[] aggregationOutputTypes;

        /**
         * Instantiates a new Hash aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param groupByInputChannels the group by input channels
         * @param groupByInputTypes the group by input types
         * @param aggregationInputChannels the aggregation input channels
         * @param aggregationInputTypes the aggregation input types
         * @param aggregatorTypes the aggregator types
         * @param aggregationOutputTypes the aggregation output types
         * @param step the step
         */
        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
            int[] groupByInputChannels, VecType[] groupByInputTypes, int[] aggregationInputChannels,
            VecType[] aggregationInputTypes, AggType[] aggregatorTypes, VecType[] aggregationOutputTypes, Step step) {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes= ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.step = step;
            this.groupByInputChannels = Arrays.copyOf(
                requireNonNull(groupByInputChannels, "groupByInputChannels is null."), groupByInputChannels.length);
            this.groupByInputTypes = Arrays.copyOf(requireNonNull(groupByInputTypes, "groupByInputTypes is null."),
                groupByInputTypes.length);
            this.aggregationInputChannels = Arrays.copyOf(
                requireNonNull(aggregationInputChannels, "aggregationInputChannels is null."),
                aggregationInputChannels.length);
            this.aggregationInputTypes = Arrays.copyOf(
                requireNonNull(aggregationInputTypes, "aggregationInputTypes is null."), aggregationInputTypes.length);
            this.aggregatorTypes = Arrays.copyOf(requireNonNull(aggregatorTypes, "aggregatorTypes is null."),
                aggregatorTypes.length);
            this.aggregationOutputTypes = Arrays.copyOf(
                requireNonNull(aggregationOutputTypes, "aggregationOutputTypes is null."),
                aggregationOutputTypes.length);

            this.omniFactory = new OmniHashAggregationOperatorFactory(createExpressions(this.groupByInputChannels),
                this.groupByInputTypes, createExpressions(this.aggregationInputChannels), this.aggregationInputTypes,
                this.aggregatorTypes, this.aggregationOutputTypes, step.isInputRaw(), step.isOutputPartial());
        }

        /**
         * Instantiates a new Hash aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param groupByInputChannels the group by input channels
         * @param groupByInputTypes the group by input types
         * @param aggregationInputChannels the aggregation input channels
         * @param aggregationInputTypes the aggregation input types
         * @param aggregatorTypes the aggregator types
         * @param inAndOutputTypes the in and output types
         */
        @VisibleForTesting
        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, int[] groupByInputChannels,
            VecType[] groupByInputTypes, int[] aggregationInputChannels, VecType[] aggregationInputTypes,
            AggType[] aggregatorTypes, List<VecType[]> inAndOutputTypes) {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            int groupByLength = groupByInputChannels.length;
            int aggLength = aggregationInputChannels.length;
            int[] cppGroupByChannels = new int[groupByLength];
            int[] cppAggChannels = new int[aggLength];

            for (int i = 0; i < groupByLength; i++) {
                cppGroupByChannels[i] = i;
            }
            for (int i = 0; i < aggLength; i++) {
                cppAggChannels[i] = groupByLength + i;
            }
            this.step = Step.SINGLE;
            OmniHashAggregationOperatorFactory omniFactory = new OmniHashAggregationOperatorFactory(
                createExpressions(cppGroupByChannels), groupByInputTypes, createExpressions(cppAggChannels),
                aggregationInputTypes, aggregatorTypes, inAndOutputTypes.get(1), this.step.isInputRaw(),
                this.step.isOutputPartial());
            this.omniFactory = omniFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            VecAllocator vecAllocator = VecAllocatorHelper.getVecAllocatorFromTaskContext(driverContext.getPipelineContext().getTaskContext());
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                HashAggregationOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniFactory.createOperator(vecAllocator);
            return new HashAggregationOmniOperator(operatorContext, omniOperator);
        }

        @Override
        public void noMoreOperators() {
        }

        @Override
        public void noMoreOperators(Lifespan lifespan) {
        }

        @Override
        public OperatorFactory duplicate() {
            return null;
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
}
