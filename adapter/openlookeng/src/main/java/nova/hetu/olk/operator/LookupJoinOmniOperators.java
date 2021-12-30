/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.collect.ImmutableList.toImmutableList;

import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import javax.inject.Inject;

/**
 * The lookup join operators.
 */
public class LookupJoinOmniOperators {
    /**
     * join type
     */
    public enum JoinType {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    /**
     * Instantiates a new Aggregation omni operator.
     */
    @Inject
    public LookupJoinOmniOperators() {
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
                probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())),
                LookupJoinOperators.JoinType.INNER, totalOperatorsCount, hashBuilderOmniOperatorFactory);
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
                probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())),
                LookupJoinOperators.JoinType.PROBE_OUTER, totalOperatorsCount, hashBuilderOmniOperatorFactory);
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
                probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())),
                LookupJoinOperators.JoinType.LOOKUP_OUTER, totalOperatorsCount, hashBuilderOmniOperatorFactory);
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
                probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())),
                LookupJoinOperators.JoinType.FULL_OUTER, totalOperatorsCount, hashBuilderOmniOperatorFactory);
    }

    private static List<Integer> rangeList(int endExclusive) {
        return IntStream.range(0, endExclusive).boxed().collect(toImmutableList());
    }

    private static OperatorFactory createOmniJoinOperatorFactory(int operatorId, PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager, List<Type> probeTypes,
            List<Integer> probeJoinChannel, OptionalInt probeHashChannel, List<Integer> probeOutputChannels,
            LookupJoinOperators.JoinType joinType, OptionalInt totalOperatorsCount,
            HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory) {
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream().map(probeTypes::get)
                .collect(toImmutableList());

        return new LookupJoinOmniOperator.LookupJoinOmniOperatorFactory(operatorId, planNodeId,
                lookupSourceFactoryManager, probeTypes, probeOutputChannels, probeOutputChannelTypes, joinType,
                totalOperatorsCount, probeJoinChannel, probeHashChannel, hashBuilderOmniOperatorFactory);
    }
}
