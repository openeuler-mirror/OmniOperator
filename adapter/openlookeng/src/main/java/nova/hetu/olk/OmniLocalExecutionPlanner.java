/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringWaitTime;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.prestosql.SystemSessionProperties.getSpillOperatorThresholdReuseExchange;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.SystemSessionProperties.isSpillOrderBy;
import static io.prestosql.SystemSessionProperties.isSpillReuseExchange;
import static io.prestosql.SystemSessionProperties.isSpillWindowOperator;
import static io.prestosql.SystemSessionProperties.getOmniAggEnabled;
import static io.prestosql.SystemSessionProperties.getOmniPartitionedOutputEnabled;
import static io.prestosql.SystemSessionProperties.getOmniTopNEnabled;
import static io.prestosql.SystemSessionProperties.getOmniOrderByEnabled;
import static io.prestosql.SystemSessionProperties.getOmniJoinEnabled;
import static io.prestosql.SystemSessionProperties.getOmniWindowEnabled;
import static io.prestosql.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_COUNT;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.execution.ExplainAnalyzeContext;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.buffer.LazyOutputBuffer;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.DynamicFilterSourceOperator;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.ExchangeOperator;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.operator.ReuseExchangeOperator;
import io.prestosql.operator.ScanFilterAndProjectOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.operator.StreamingAggregationOperator;
import io.prestosql.operator.TableScanOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskOutputOperator;
import io.prestosql.operator.ValuesOperator;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSinkOperator;
import io.prestosql.operator.exchange.LocalExchangeSourceOperator;
import io.prestosql.operator.exchange.LocalMergeSourceOperator;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.operator.TopNOperator;
import io.prestosql.operator.PartitionedOutputOperator;
import io.prestosql.operator.AggregationOperator;
import io.prestosql.operator.MergeOperator;
import io.prestosql.operator.HashAggregationOperator;
import io.prestosql.operator.OrderByOperator;
import io.prestosql.operator.WindowOperator;
import io.prestosql.operator.HashBuilderOperator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.SortExpressionContext;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import nova.hetu.olk.block.InternalOmniBlockEncodingSerde;
import nova.hetu.olk.operator.AggregationOmniOperator;
import nova.hetu.olk.operator.HashAggregationOmniOperator;
import nova.hetu.olk.operator.HashBuilderOmniOperator;
import nova.hetu.olk.operator.LocalMergeSourceOmniOperator;
import nova.hetu.olk.operator.LookupJoinOmniOperator;
import nova.hetu.olk.operator.MergeOmniOperator;
import nova.hetu.olk.operator.PartitionedOutputOmniOperator;
import nova.hetu.olk.operator.TopNOmniOperator;
import nova.hetu.olk.operator.WindowOmniOperator;
import nova.hetu.olk.operator.filterandproject.FilterAndProjectOmniOperator;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
import nova.hetu.olk.operator.filterandproject.OmniPageProcessor;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.type.ContainerVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;
import nova.hetu.shuffle.PageProducer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The type Omni local execution planner.
 *
 * @since 20210630
 */
public class OmniLocalExecutionPlanner extends LocalExecutionPlanner {
    private static final Logger log = Logger.get(OmniLocalExecutionPlanner.class);

    private final Metadata metadata;

    private final TypeAnalyzer typeAnalyzer;

    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;

    private final PageSourceProvider pageSourceProvider;

    private final IndexManager indexManager;

    private final NodePartitioningManager nodePartitioningManager;

    private final PageSinkManager pageSinkManager;

    private final ExchangeClientSupplier exchangeClientSupplier;

    private final ExpressionCompiler expressionCompiler;

    private final PageFunctionCompiler pageFunctionCompiler;

    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;

    private final DataSize maxIndexMemorySize;

    private final IndexJoinLookupStats indexJoinLookupStats;

    private final DataSize maxPartialAggregationMemorySize;

    private final DataSize maxPagePartitioningBufferSize;

    private final DataSize maxLocalExchangeBufferSize;

    private final SpillerFactory spillerFactory;

    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final PagesIndex.Factory pagesIndexFactory;

    private final JoinCompiler joinCompiler;

    private final LookupJoinOperators lookupJoinOperators;

    private final OrderingCompiler orderingCompiler;

    private final StateStoreProvider stateStoreProvider;

    private final NodeInfo nodeInfo;

    private final StateStoreListenerManager stateStoreListenerManager;

    private final DynamicFilterCacheManager dynamicFilterCacheManager;

    private final HeuristicIndexerManager heuristicIndexerManager;

    /**
     * The Support types.
     */
    Set<String> supportTypes = new HashSet<String>() {{
        add(StandardTypes.INTEGER);
        add(StandardTypes.DATE);
        add(StandardTypes.BIGINT);
        add(StandardTypes.VARCHAR);
        add(StandardTypes.CHAR);
        add(StandardTypes.DECIMAL);
        add(StandardTypes.ROW);
        add(StandardTypes.DOUBLE);
        add(StandardTypes.VARBINARY);
    }};

    /**
     * Instantiates a new Omni local execution planner.
     *
     * @param metadata the metadata
     * @param typeAnalyzer the type analyzer
     * @param explainAnalyzeContext the explain analyze context
     * @param pageSourceProvider the page source provider
     * @param indexManager the index manager
     * @param nodePartitioningManager the node partitioning manager
     * @param pageSinkManager the page sink manager
     * @param exchangeClientSupplier the exchange client supplier
     * @param expressionCompiler the expression compiler
     * @param pageFunctionCompiler the page function compiler
     * @param joinFilterFunctionCompiler the join filter function compiler
     * @param indexJoinLookupStats the index join lookup stats
     * @param taskManagerConfig the task manager config
     * @param spillerFactory the spiller factory
     * @param singleStreamSpillerFactory the single stream spiller factory
     * @param partitioningSpillerFactory the partitioning spiller factory
     * @param pagesIndexFactory the pages index factory
     * @param joinCompiler the join compiler
     * @param lookupJoinOperators the lookup join operators
     * @param orderingCompiler the ordering compiler
     * @param nodeInfo the node info
     * @param stateStoreProvider the state store provider
     * @param stateStoreListenerManager the state store listener manager
     * @param dynamicFilterCacheManager the dynamic filter cache manager
     * @param heuristicIndexerManager the heuristic indexer manager
     */
    public OmniLocalExecutionPlanner(Metadata metadata, TypeAnalyzer typeAnalyzer,
        Optional<ExplainAnalyzeContext> explainAnalyzeContext, PageSourceProvider pageSourceProvider,
        IndexManager indexManager, NodePartitioningManager nodePartitioningManager, PageSinkManager pageSinkManager,
        ExchangeClientSupplier exchangeClientSupplier, ExpressionCompiler expressionCompiler,
        PageFunctionCompiler pageFunctionCompiler, JoinFilterFunctionCompiler joinFilterFunctionCompiler,
        IndexJoinLookupStats indexJoinLookupStats, TaskManagerConfig taskManagerConfig, SpillerFactory spillerFactory,
        SingleStreamSpillerFactory singleStreamSpillerFactory, PartitioningSpillerFactory partitioningSpillerFactory,
        PagesIndex.Factory pagesIndexFactory, JoinCompiler joinCompiler, LookupJoinOperators lookupJoinOperators,
        OrderingCompiler orderingCompiler, NodeInfo nodeInfo, StateStoreProvider stateStoreProvider,
        StateStoreListenerManager stateStoreListenerManager, DynamicFilterCacheManager dynamicFilterCacheManager,
        HeuristicIndexerManager heuristicIndexerManager) {
        super(metadata, typeAnalyzer, explainAnalyzeContext, pageSourceProvider, indexManager, nodePartitioningManager,
            pageSinkManager, exchangeClientSupplier, expressionCompiler, pageFunctionCompiler,
            joinFilterFunctionCompiler, indexJoinLookupStats, taskManagerConfig, spillerFactory,
            singleStreamSpillerFactory, partitioningSpillerFactory, pagesIndexFactory, joinCompiler,
            lookupJoinOperators, orderingCompiler, nodeInfo, stateStoreProvider, stateStoreListenerManager,
            dynamicFilterCacheManager, heuristicIndexerManager);
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        // use omni expressionCompiler
        this.expressionCompiler = new OmniExpressionCompiler(metadata, pageFunctionCompiler);
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "compiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig,
            "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory,
            "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory,
            "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.lookupJoinOperators = requireNonNull(lookupJoinOperators, "lookupJoinOperators is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStore is null");
        this.nodeInfo = nodeInfo;
        this.stateStoreListenerManager = requireNonNull(stateStoreListenerManager, "stateStoreListenerManager is null");
        this.dynamicFilterCacheManager = requireNonNull(dynamicFilterCacheManager, "dynamicFilterCacheManager is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
    }

    @Override
    public LocalExecutionPlan plan(TaskContext taskContext, PlanNode plan, TypeProvider types,
        PartitioningScheme partitioningScheme, StageExecutionDescriptor stageExecutionDescriptor,
        List<PlanNodeId> partitionedSourceOrder, OutputBuffer outputBuffer, List<PageProducer> pageProducers) {
        TaskId taskId = taskContext.getTaskId();
        VecAllocator vecAllocator = VecAllocatorFactory.create(taskId.getFullId(), () -> {
            taskContext.getTaskStateMachine().addStateChangeListenerToTail(state -> {
                if (state.isDone()) {
                    VecAllocatorFactory.delete(taskId.getFullId());
                }
            });
        });

        VecAllocatorHelper.setVectorAllocatorToTaskContext(taskContext, vecAllocator);

        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (outputBuffer instanceof LazyOutputBuffer
            && ((LazyOutputBuffer) outputBuffer).getDelegate().getSerde() != null) {
            ((LazyOutputBuffer) outputBuffer).getDelegate()
                .getSerde()
                .setBlockEncodingSerde(new InternalOmniBlockEncodingSerde(metadata, taskId));
        }

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)
            || partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)
            || partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) || partitioningScheme
            .getPartitioning()
            .getHandle()
            .equals(SINGLE_DISTRIBUTION) || partitioningScheme.getPartitioning()
            .getHandle()
            .equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder,
                pageProducers, new TaskOutputOperator.TaskOutputFactory(outputBuffer, pageProducers));
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        } else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return -1;
                }
                return outputLayout.indexOf(argument.getColumn());
            }).collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return Optional.of(argument.getConstant());
                }
                return Optional.<NullableValue>empty();
            }).collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return argument.getConstant().getType();
                }
                return types.get(argument.getColumn());
            }).collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(),
            partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        boolean isHashPrecomputed = partitioningScheme.getHashColumn().isPresent();
        if (getOmniPartitionedOutputEnabled(taskContext.getSession())) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder,
                    pageProducers,
                    new PartitionedOutputOmniOperator.PartitionedOutputOmniFactory(partitionFunction, partitionChannels,
                            partitionConstants, partitioningScheme.isReplicateNullsAndAny(), nullChannel, outputBuffer,
                            pageProducers, maxPagePartitioningBufferSize, partitioningScheme.getBucketToPartition().get(),
                            isHashPrecomputed, partitionChannelTypes));
        } else {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder,
                    pageProducers,
                    new PartitionedOutputOperator.PartitionedOutputFactory(
                            partitionFunction,
                            partitionChannels,
                            partitionConstants,
                            partitioningScheme.isReplicateNullsAndAny(),
                            nullChannel,
                            outputBuffer,
                            pageProducers,
                            maxPagePartitioningBufferSize));
        }
    }

    @Override
    public LocalExecutionPlan plan(TaskContext taskContext, StageExecutionDescriptor stageExecutionDescriptor,
        PlanNode plan, List<Symbol> outputLayout, TypeProvider types, List<PlanNodeId> partitionedSourceOrder,
        List<PageProducer> outputStreams, OutputFactory outputOperatorFactory) {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types, metadata,
            dynamicFilterCacheManager);

        PhysicalOperation physicalOperation;
        try {
            physicalOperation = plan.accept(new OmniVisitor(session, stageExecutionDescriptor), context);
        } catch (Exception e) {
            return null;
        }

        for (DriverFactory driverFactory : context.getDriverFactories()) {
            for (OperatorFactory operatorFactory : driverFactory.getOperatorFactories()) {
                // if the operator is source or output operator then continue
                if (operatorFactory instanceof TableScanOperator.TableScanOperatorFactory
                    || operatorFactory instanceof ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory
                    || operatorFactory instanceof ExchangeOperator.ExchangeOperatorFactory
                    || operatorFactory instanceof LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory
                    || operatorFactory instanceof LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory
                    || operatorFactory instanceof TaskOutputOperator.TaskOutputOperatorFactory
                    || operatorFactory instanceof DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory
                    || operatorFactory instanceof ValuesOperator.ValuesOperatorFactory) {
                    continue;
                }

                // if there is a operator not support by OmniRuntime, then fall back
                // if there is a data type not support by OmniRuntime, then fall back
                if (notSupportTypes(operatorFactory.getSourceTypes())) {
                    log.warn("There is a data type not support by OmniRuntime: %s",
                        operatorFactory.getSourceTypes().toString());
                    return null;
                }
            }
        }

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream().map(types::get).collect(toImmutableList());

        context.addDriverFactory(context.isInputDriver(), true, ImmutableList.<OperatorFactory>builder()
            .addAll(physicalOperation.getOperatorFactories())
            .add(outputOperatorFactory.createOutputOperator(context.getNextOperatorId(), plan.getId(), outputTypes,
                pagePreprocessor,
                new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session))))
            .build(), context.getDriverInstanceCount(), physicalOperation.getPipelineExecutionStrategy());

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories()
            .stream()
            .map(DriverFactory::getOperatorFactories)
            .flatMap(List::stream)
            .filter(LocalPlannerAware.class::isInstance)
            .map(LocalPlannerAware.class::cast)
            .forEach(LocalPlannerAware::localPlannerComplete);

        log.debug("create the omni local execution plan successful!");
        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor);
    }

    private boolean notSupportTypes(List<Type> types) {
        for (Type type : types) {
            String base = type.getTypeSignature().getBase();
            if (!supportTypes.contains(base)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The type Omni visitor.
     *
     * @since 20210630
     */
    public class OmniVisitor extends Visitor {
        private final Session session;

        private final StageExecutionDescriptor stageExecutionDescriptor;

        /**
         * Instantiates a new Omni visitor.
         *
         * @param session the session
         * @param stageExecutionDescriptor the stage execution descriptor
         */
        public OmniVisitor(Session session, StageExecutionDescriptor stageExecutionDescriptor) {
            super(session, stageExecutionDescriptor);
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context) {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().getOrdering(symbol));
            }

            OperatorFactory operatorFactory;
            if (getOmniTopNEnabled(session)) {
                operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(context.getNextOperatorId(), node.getId(),
                        source.getTypes(), (int) node.getCount(), sortChannels, sortOrders);
            } else {
                operatorFactory = new TopNOperator.TopNOperatorFactory(context.getNextOperatorId(), node.getId(),
                        source.getTypes(), (int) node.getCount(), sortChannels, sortOrders);
            }

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context) {
            PlanNode sourceNode = node.getSource();

            Expression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression),
                Assignments.identity(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context) {
            PlanNode sourceNode;
            Optional<Expression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            } else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(),
                outputSymbols);
        }

        private PhysicalOperation visitScanFilterAndProject(LocalExecutionPlanContext context, PlanNodeId planNodeId,
            PlanNode sourceNode, Optional<Expression> filterExpression, Assignments assignments,
            List<Symbol> outputSymbols) {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            Integer reuseTableScanMappingId = 0;
            Integer consumerTableScanNodeCount = 0;
            List<Type> inputTypes;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                inputTypes = getSymbolTypes(tableScanNode.getOutputSymbols(), context.getTypes());

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            // TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported",
                    sampleNode.getSampleType());
                return visitScanFilterAndProject(context, planNodeId, sampleNode.getSource(), filterExpression,
                    assignments, outputSymbols);
            } else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();

                inputTypes = source.getTypes();
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression.map(
                DynamicFilters::extractDynamicFilters);
            Optional<Expression> staticFilters = extractDynamicFilterResult.map(
                DynamicFilters.ExtractResult::getStaticConjuncts).map(ExpressionUtils::combineConjuncts);

            // TODO: Execution must be plugged in here
            Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier = getDynamicFilterSupplier(
                extractDynamicFilterResult, sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if (dynamicFilterSupplier != null) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(),
                    getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<Expression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(context.getSession(),
                context.getTypes(),
                concat(staticFilters.map(ImmutableList::of).orElse(ImmutableList.of()), assignments.getExpressions()));

            Optional<RowExpression> translatedFilter = staticFilters.map(
                filter -> toRowExpression(filter, expressionTypes, sourceLayout));
            List<RowExpression> translatedProjections = projections.stream()
                .map(expression -> toRowExpression(expression, expressionTypes, sourceLayout))
                .collect(toImmutableList());

            if (columns != null) {
                // TODO: Need Support RecordCursor Filter And Project Omni Codegen

                OmniExpressionCompiler omniExpressionCompiler = new OmniExpressionCompiler(metadata,
                    pageFunctionCompiler);
                Supplier<OmniPageProcessor> oPageProcessor = omniExpressionCompiler.getOmniPageProcessor(
                    translatedFilter, translatedProjections, inputTypes, context.getTaskId());
                OmniPageProcessor omniPageProcessor = oPageProcessor.get();
                OmniOperatorFactory omniOperatorFactory = omniPageProcessor.getProjection().getFactory();
                if (omniOperatorFactory.getNativeOperatorFactory() == 0) {
                    throw new UnsupportedOperationException("This expression is not supported by OmniRuntime");
                }

                boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024
                    * 1024; // convert from MB to bytes

                Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter,
                    translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId), OptionalInt.empty(),
                    inputTypes, context.getTaskId());
                Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter,
                    translatedProjections, sourceNode.getId());
                SourceOperatorFactory operatorFactory
                    = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(context.getSession(),
                    context.getNextOperatorId(), planNodeId, sourceNode, pageSourceProvider, cursorProcessor,
                    pageProcessor, table, columns, dynamicFilter, getTypes(projections, expressionTypes),
                    stateStoreProvider, metadata, dynamicFilterCacheManager,
                    getFilterAndProjectMinOutputPageSize(session), getFilterAndProjectMinOutputPageRowCount(session),
                    strategy, reuseTableScanMappingId, spillEnabled, Optional.of(spillerFactory), spillerThreshold,
                    consumerTableScanNodeCount);

                return new PhysicalOperation(operatorFactory, outputMappings, context,
                    stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId())
                        ? GROUPED_EXECUTION
                        : UNGROUPED_EXECUTION);
            } else {
                OmniExpressionCompiler omniExpressionCompiler = new OmniExpressionCompiler(metadata,
                    pageFunctionCompiler);
                Supplier<OmniPageProcessor> oPageProcessor = omniExpressionCompiler.getOmniPageProcessor(
                    translatedFilter, translatedProjections, inputTypes, context.getTaskId());
                OmniPageProcessor omniPageProcessor = oPageProcessor.get();
                OmniOperatorFactory omniOperatorFactory = omniPageProcessor.getProjection().getFactory();
                if (omniOperatorFactory.getNativeOperatorFactory() == 0) {
                    throw new UnsupportedOperationException("This expression is not supported by OmniRuntime");
                }

                Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter,
                    translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId), OptionalInt.empty(),
                    inputTypes, context.getTaskId());
                OperatorFactory operatorFactory = new FilterAndProjectOmniOperator.FilterAndProjectOmniOperatorFactory(
                    context.getNextOperatorId(), planNodeId, pageProcessor, getTypes(projections, expressionTypes),
                    getFilterAndProjectMinOutputPageSize(session), getFilterAndProjectMinOutputPageRowCount(session), session);

                return new PhysicalOperation(operatorFactory, outputMappings, context, source);
            }
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context) {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            Assignments assignments = Assignments.identity(node.getOutputSymbols());
            Map<NodeRef<Expression>, Type> columnTypes = typeAnalyzer.getTypes(context.getSession(), context.getTypes(),
                concat(assignments.getExpressions()));

            boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
            int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024
                * 1024; // convert from MB to bytes
            Integer consumerTableScanNodeCount = node.getConsumerTableScanNodeCount();

            OperatorFactory operatorFactory = new TableScanOperator.TableScanOperatorFactory(context.getSession(),
                context.getNextOperatorId(), node, pageSourceProvider, node.getTable(), columns,
                columnTypes.values().stream().collect(Collectors.toList()), stateStoreProvider, metadata,
                dynamicFilterCacheManager, getFilterAndProjectMinOutputPageSize(session),
                getFilterAndProjectMinOutputPageRowCount(session), node.getStrategy(),
                node.getReuseTableScanMappingId(), spillEnabled, Optional.of(spillerFactory), spillerThreshold,
                consumerTableScanNodeCount);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context,
                stageExecutionDescriptor.isScanGroupedExecution(node.getId())
                    ? GROUPED_EXECUTION
                    : UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context) {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context) {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            LocalExchange.LocalExchangeFactory exchangeFactory = new LocalExchange.LocalExchangeFactory(
                node.getPartitioningScheme().getPartitioning().getHandle(), operatorsCount, types, ImmutableList.of(),
                Optional.empty(), source.getPipelineExecutionStrategy(), maxLocalExchangeBufferSize);

            List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());
            List<Symbol> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
            operatorFactories.add(new LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory(exchangeFactory,
                subContext.getNextOperatorId(), node.getId(), exchangeFactory.newSinkFactoryId(), pagePreprocessor));
            context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories,
                subContext.getDriverInstanceCount(), source.getPipelineExecutionStrategy());
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }
            OperatorFactory operatorFactory;
            if (getOmniOrderByEnabled(context.getSession())) {
                operatorFactory = new LocalMergeSourceOmniOperator.LocalMergeSourceOmniOperatorFactory(
                    context.getNextOperatorId(), context.getNextOperatorId(), node.getId(), exchangeFactory, types,
                    orderingCompiler, sortChannels, orderings, outputChannels.build());
            } else {
                operatorFactory = new LocalMergeSourceOperator.LocalMergeSourceOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        exchangeFactory,
                        types,
                        orderingCompiler,
                        sortChannels,
                        orderings);
            }
            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context) {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context) {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size()).boxed().collect(toImmutableList());

            OperatorFactory operatorFactory;
            if (getOmniOrderByEnabled(session)) {
                operatorFactory = new MergeOmniOperator.MergeOmniOperatorFactory(
                        context.getNextOperatorId(), context.getNextOperatorId(), node.getId(), exchangeClientSupplier,
                        new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)),
                        orderingCompiler, types, outputChannels, sortChannels, sortOrder);
            } else {
                operatorFactory = new MergeOperator.MergeOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        exchangeClientSupplier,
                        new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)),
                        orderingCompiler,
                        types,
                        outputChannels,
                        sortChannels,
                        sortOrder);
            }

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context) {
            if (!context.getDriverInstanceCount().isPresent()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(metadata, context.getTaskId());
            log.debug("using OmniInternalBlockEncodingSerde!");
            OperatorFactory operatorFactory = new ExchangeOperator.ExchangeOperatorFactory(context.getNextOperatorId(),
                node.getId(), exchangeClientSupplier,
                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)));

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context) {
            PhysicalOperation source = node.getSource().accept(this, context);
            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession());
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(context.getSession());

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source,
            LocalExecutionPlanContext context) {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createAggregationOperatorFactory(node.getId(), node.getAggregations(),
                node.getStep(), 0, outputMappings, source, context, node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private OperatorFactory createAggregationOperatorFactory(PlanNodeId planNodeId,
            Map<Symbol, AggregationNode.Aggregation> aggregations, AggregationNode.Step step, int startOutputChannel,
            ImmutableMap.Builder<Symbol, Integer> outputMappings, PhysicalOperation source,
            LocalExecutionPlanContext context, boolean useSystemMemory) {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AccumulatorFactory> accumulatorFactories = ImmutableList.builder();
            ImmutableList.Builder<AggregationNode.Aggregation> aggregationBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                AggregationNode.Aggregation aggregation = entry.getValue();
                aggregationBuilder.add(aggregation);
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }
            ImmutableList<AccumulatorFactory> factories = accumulatorFactories.build();
            ImmutableList<AggregationNode.Aggregation> aggregationToOmni = aggregationBuilder.build();
            if (getOmniAggEnabled(session)) {
                // use omni aggregation operator
                return new AggregationOmniOperator.AggregationOmniOperatorFactory(context.getNextOperatorId(), planNodeId,
                        source.getTypes(), aggregationToOmni, factories, step);
            }
            return new AggregationOperator.AggregationOperatorFactory(context.getNextOperatorId(), planNodeId,
                    step, factories, useSystemMemory);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source,
            boolean spillEnabled, DataSize unspillMemoryLimit, LocalExecutionPlanContext context) {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(node.getId(), node.getAggregations(),
                node.getGlobalGroupingSets(), node.getGroupingKeys(), node.getStep(), node.getHashSymbol(),
                node.getGroupIdSymbol(), source, node.hasDefaultOutput(), spillEnabled, node.isStreamable(),
                unspillMemoryLimit, context, 0, mappings, 10_000, Optional.of(maxPartialAggregationMemorySize),
                node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, mappings.build(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(PlanNodeId planNodeId,
            Map<Symbol, AggregationNode.Aggregation> aggregations, Set<Integer> globalGroupingSets,
            List<Symbol> groupBySymbols, AggregationNode.Step step, Optional<Symbol> hashSymbol,
            Optional<Symbol> groupIdSymbol, PhysicalOperation source, boolean hasDefaultOutput, boolean spillEnabled,
            boolean isStreamable, DataSize unspillMemoryLimit, LocalExecutionPlanContext context,
            int startOutputChannel, ImmutableMap.Builder<Symbol, Integer> outputMappings, int expectedGroups,
            Optional<DataSize> maxPartialAggregationMemorySize, boolean useSystemMemory) {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                AggregationNode.Aggregation aggregation = entry.getValue();

                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashSymbol.isPresent()) {
                outputMappings.put(hashSymbol.get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                .map(entry -> source.getTypes().get(entry))
                .collect(toImmutableList());

            if (isStreamable) {
                return new StreamingAggregationOperator.StreamingAggregationOperatorFactory(context.getNextOperatorId(),
                    planNodeId, source.getTypes(), groupByTypes, groupByChannels, step, accumulatorFactories,
                    joinCompiler);
            } else {
                Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
                if (getOmniAggEnabled(session)) {
                    // when omni is turned on there is no hash channel
                    int[] groupByInputChannels = Ints.toArray(groupByChannels);
                    VecType[] groupByInputTypes = OperatorUtils.toVecTypes(groupByTypes);
                    int aggregationSize = aggregationOutputSymbols.size();
                    int[] aggregationInputChannels = new int[aggregationSize];
                    VecType[] aggregationInputTypes = new VecType[aggregationSize];
                    AggType[] aggregatorTypes = new AggType[aggregationSize];
                    VecType[] aggregationOutputTypes = new VecType[aggregationSize];

                    // TODO move this logic to native side
                    int countStarIndex = -1;
                    int actualIndex = 0;
                    for (int i = 0; i < aggregationSize; ++i) {
                        Signature signature = aggregations.get(aggregationOutputSymbols.get(i)).getSignature();
                        AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
                        List<Integer> inputChannels = accumulatorFactory.getInputChannels();

                    if ("count".equals(signature.getName()) && inputChannels.size() == 0) {
                        // for count(*) need to count on another input channel
                        countStarIndex = groupByInputChannels[0];
                        aggregationInputTypes[i] = groupByInputTypes[0];
                        aggregationInputChannels[i] = groupByInputChannels[0];
                    } else if ("count".equals(signature.getName())) {
                        actualIndex = groupByChannels.get(0);
                        aggregationInputTypes[i] = groupByInputTypes[0];
                        aggregationInputChannels[i] = inputChannels.get(0);
                    } else if (step == FINAL && "avg".equals(signature.getName())) {
                        aggregationInputTypes[i] = ContainerVecType.CONTAINER;
                        actualIndex = i;
                        aggregationInputChannels[i] = inputChannels.get(0);
                    } else {
                        aggregationInputTypes[i] = OperatorUtils.toVecType(signature.getArgumentTypes().get(0));
                        actualIndex = i;
                        aggregationInputChannels[i] = inputChannels.get(0);
                    }
                    // return types
                    aggregationOutputTypes[i] = OperatorUtils.toVecType(signature.getReturnType());
                }

                    for (int i = 0; i < aggregationSize; i++) {
                        Signature signature = aggregations.get(aggregationOutputSymbols.get(i)).getSignature();
                        // aggregator type, eg:sum,avg...
                        switch (signature.getName()) {
                            case "sum":
                                aggregatorTypes[i] = OMNI_AGGREGATION_TYPE_SUM;
                                break;
                            case "avg":
                                aggregatorTypes[i] = OMNI_AGGREGATION_TYPE_AVG;
                                break;
                            case "count":
                                aggregatorTypes[i] = OMNI_AGGREGATION_TYPE_COUNT;
                                break;
                            case "min":
                                aggregatorTypes[i] = OMNI_AGGREGATION_TYPE_MIN;
                                break;
                            case "max":
                                aggregatorTypes[i] = OMNI_AGGREGATION_TYPE_MAX;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "unsupported Aggregator type by OmniRuntime: " + signature.getName());
                        }
                    }
                    return new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(context.getNextOperatorId(),
                            planNodeId, source.getTypes(), groupByInputChannels, groupByInputTypes, aggregationInputChannels,
                            aggregationInputTypes, aggregatorTypes, aggregationOutputTypes, step);
                }
                return new HashAggregationOperator.HashAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        groupByTypes,
                        groupByChannels,
                        ImmutableList.copyOf(globalGroupingSets),
                        step,
                        hasDefaultOutput,
                        accumulatorFactories,
                        hashChannel,
                        groupIdChannel,
                        expectedGroups,
                        maxPartialAggregationMemorySize,
                        spillEnabled,
                        unspillMemoryLimit,
                        spillerFactory,
                        joinCompiler,
                        useSystemMemory);
            }
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context) {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().getOrdering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession()) && isSpillOrderBy(context.getSession());

            OperatorFactory operator;
            if (getOmniOrderByEnabled(context.getSession())) {
                operator = createOrderByOmniOperatorFactory(context.getNextOperatorId(), node.getId(),
                        source.getTypes(), outputChannels.build(), orderByChannels, sortOrder.build());
            } else {
                operator = new OrderByOperator.OrderByOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        source.getTypes(),
                        outputChannels.build(),
                        10_000,
                        orderByChannels,
                        sortOrder.build(),
                        pagesIndexFactory,
                        spillEnabled,
                        Optional.of(spillerFactory),
                        orderingCompiler);
            }
            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context) {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(
                getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(
                getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();

                WindowNode.Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }

                FrameInfo frameInfo = new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel,
                    frame.getEndType(), frameEndChannel);

                Signature signature = entry.getValue().getSignature();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (Expression argument : entry.getValue().getArguments()) {
                    Symbol argumentSymbol = Symbol.from(argument);
                    arguments.add(source.getLayout().get(argumentSymbol));
                }
                Symbol symbol = entry.getKey();
                WindowFunctionSupplier windowFunctionSupplier = metadata.getWindowFunctionImplementation(signature);
                Type type = metadata.getType(signature.getReturnType());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            OperatorFactory operatorFactory;
            if (getOmniWindowEnabled(session)) {
                operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                        context.getNextOperatorId(), node.getId(), source.getTypes(), outputChannels.build(),
                        windowFunctionsBuilder.build(), partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                        node.getPreSortedOrderPrefix(), 10_000);
            } else {
                operatorFactory = new WindowOperator.WindowOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        source.getTypes(),
                        outputChannels.build(),
                        windowFunctionsBuilder.build(),
                        partitionChannels,
                        preGroupedChannels,
                        sortChannels,
                        sortOrder,
                        node.getPreSortedOrderPrefix(),
                        10_000,
                        pagesIndexFactory,
                        isSpillEnabled(session) && isSpillWindowOperator(session),
                        spillerFactory,
                        orderingCompiler);
            }
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context) {
            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // TODO: Execution must be plugged in here
            if (!node.getDynamicFilters().isEmpty()) {
                // log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            }

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(),
                        node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        public JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(JoinNode node,
            PlanNode buildNode, List<Symbol> buildSymbols, Optional<Symbol> buildHashSymbol,
            PhysicalOperation probeSource, LocalExecutionPlanContext context, boolean spillEnabled) {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                    "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols()
                .stream()
                .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                .collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList.copyOf(
                getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(
                getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int taskCount = buildContext.getDriverInstanceCount().orElse(1);

            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                .map(filterExpression -> compileJoinFilterFunction(filterExpression, probeSource.getLayout(),
                    buildSource.getLayout(), context.getTypes(), context.getSession()));

            Optional<SortExpressionContext> sortExpressionContext = node.getSortExpressionContext();

            Optional<Integer> sortChannel = sortExpressionContext.map(SortExpressionContext::getSortExpression)
                .map(Symbol::from)
                .map(sortSymbol -> createJoinSourcesLayout(buildSource.getLayout(), probeSource.getLayout()).get(
                    sortSymbol));

            List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories
                = sortExpressionContext.map(SortExpressionContext::getSearchExpressions)
                .map(searchExpressions -> searchExpressions.stream()
                    .map(searchExpression -> compileJoinFilterFunction(searchExpression, probeSource.getLayout(),
                        buildSource.getLayout(), context.getTypes(), context.getSession()))
                    .collect(toImmutableList()))
                .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                .map(buildSource.getTypes()::get)
                .collect(toImmutableList());
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                buildOuter, probeSource.getPipelineExecutionStrategy(), buildSource.getPipelineExecutionStrategy(),
                lifespan -> new PartitionedLookupSourceFactory(buildSource.getTypes(), buildOutputTypes,
                    buildChannels.stream().map(buildSource.getTypes()::get).collect(toImmutableList()), taskCount,
                    buildSource.getLayout(), buildOuter), buildOutputTypes);

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = new ImmutableList.Builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(filter -> {
                List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter.getBuildChannels()
                    .entrySet()
                    .stream()
                    .map(entry -> {
                        String filterId = entry.getKey();
                        int index = entry.getValue();
                        Type type = buildSource.getTypes().get(index);
                        return new DynamicFilterSourceOperator.Channel(filterId, type, index,
                            context.getSession().getQueryId().toString());
                    })
                    .collect(Collectors.toList());
                factoriesBuilder.add(
                    new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(buildContext.getNextOperatorId(),
                        node.getId(), filter.getValueConsumer(),
                        /** the consumer to process all values collected to build the dynamic filter */
                        filterBuildChannels, getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
            });
            if (getOmniJoinEnabled(context.getSession())) {
                HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory
                        = new HashBuilderOmniOperator.HashBuilderOmniOperatorFactory(buildContext.getNextOperatorId(),
                        node.getId(), lookupSourceFactoryManager, buildSource.getTypes(), buildOutputChannels, buildChannels,
                        buildHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, taskCount);
                factoriesBuilder.add(hashBuilderOmniOperatorFactory);
            }else {
                HashBuilderOperator.HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperator.HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactoryManager,
                        buildOutputChannels,
                        buildChannels,
                        buildHashChannel,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories,
                        10_000,
                        pagesIndexFactory,
                        spillEnabled && !buildOuter && taskCount > 1,
                        singleStreamSpillerFactory);

                factoriesBuilder.add(hashBuilderOperatorFactory);
            }


            context.addDriverFactory(buildContext.isInputDriver(), false, factoriesBuilder.build(),
                buildContext.getDriverInstanceCount(), buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }

        @Override
        public PhysicalOperation createLookupJoin(JoinNode node, PlanNode probeNode, List<Symbol> probeSymbols,
            Optional<Symbol> probeHashSymbol, PlanNode buildNode, List<Symbol> buildSymbols,
            Optional<Symbol> buildHashSymbol, LocalExecutionPlanContext context) {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean spillEnabled = isSpillEnabled(session) && node.isSpillable()
                .orElseThrow(() -> new IllegalArgumentException("spillable not yet set"));
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = createLookupSourceFactory(node,
                buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled);

            OperatorFactory operator = createLookupJoin(node, probeSource, probeSymbols, probeHashSymbol,
                lookupSourceFactory, context, spillEnabled);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        @Override
        public OperatorFactory createLookupJoin(JoinNode node, PhysicalOperation probeSource, List<Symbol> probeSymbols,
            Optional<Symbol> probeHashSymbol,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            LocalExecutionPlanContext context, boolean spillEnabled) {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols()
                .stream()
                .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(
                getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(
                getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource))
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            checkState(!spillEnabled || totalOperatorsCount.isPresent(),
                "A fixed distribution is required for JOIN when spilling is enabled");
            if (getOmniJoinEnabled(context.getSession())) {
                return createOmniLookupJoin(node, lookupSourceFactoryManager, context, probeTypes, probeOutputChannels,
                        probeJoinChannels, probeHashChannel, totalOperatorsCount);
            }
            return getLookUpJoinOperatorFactory(node, lookupSourceFactoryManager, context, probeTypes, probeOutputChannels, probeJoinChannels, probeHashChannel, totalOperatorsCount);
        }

        /**
         * Create omni lookup join operator factory.
         *
         * @param node the node
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param context the context
         * @param probeTypes the probe types
         * @param probeOutputChannels the probe output channels
         * @param probeJoinChannels the probe join channels
         * @param probeHashChannel the probe hash channel
         * @param totalOperatorsCount the total operators count
         * @return the operator factory
         */
        public OperatorFactory createOmniLookupJoin(JoinNode node,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            LocalExecutionPlanContext context, List<Type> probeTypes, List<Integer> probeOutputChannels,
            List<Integer> probeJoinChannels, OptionalInt probeHashChannel, OptionalInt totalOperatorsCount) {
            List<DriverFactory> driverFactories = context.getDriverFactories();
            DriverFactory driverFactory = driverFactories.get(driverFactories.size() - 1);
            List<OperatorFactory> operatorFactories = driverFactory.getOperatorFactories();
            OperatorFactory buildOperatorFactory = operatorFactories.get(operatorFactories.size() - 1);

            switch (node.getType()) {
                case INNER:
                    return LookupJoinOmniOperator.innerJoin(context.getNextOperatorId(), node.getId(),
                        lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                        Optional.of(probeOutputChannels), totalOperatorsCount,
                        (HashBuilderOmniOperator.HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case LEFT:
                    return LookupJoinOmniOperator.probeOuterJoin(context.getNextOperatorId(), node.getId(),
                        lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                        Optional.of(probeOutputChannels), totalOperatorsCount,
                        (HashBuilderOmniOperator.HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case RIGHT:
                    return LookupJoinOmniOperator.lookupOuterJoin(context.getNextOperatorId(), node.getId(),
                        lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                        Optional.of(probeOutputChannels), totalOperatorsCount,
                        (HashBuilderOmniOperator.HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case FULL:
                    return LookupJoinOmniOperator.fullOuterJoin(context.getNextOperatorId(), node.getId(),
                        lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                        Optional.of(probeOutputChannels), totalOperatorsCount,
                        (HashBuilderOmniOperator.HashBuilderOmniOperatorFactory) buildOperatorFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node) {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols) {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.build();
        }
    }

    private OperatorFactory getLookUpJoinOperatorFactory(JoinNode node, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager, LocalExecutionPlanContext context, List<Type> probeTypes, List<Integer> probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, OptionalInt totalOperatorsCount) {
        switch (node.getType()) {
            case INNER:
                return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
            case LEFT:
                return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
            case RIGHT:
                return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
            case FULL:
                return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
        }
    }
}
