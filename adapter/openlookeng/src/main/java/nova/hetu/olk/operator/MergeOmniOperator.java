/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.metadata.Split;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.SourceOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The type Merge omni operator.
 *
 * @since 20210630
 */
public class MergeOmniOperator implements SourceOperator, Closeable {
    /**
     * The type Merge omni operator factory.
     *
     * @since 20210630
     */
    public static class MergeOmniOperatorFactory implements SourceOperatorFactory {
        private final int operatorId;

        private final PlanNodeId sourceId;

        private final ExchangeClientSupplier exchangeClientSupplier;

        private final PagesSerdeFactory serdeFactory;

        private final List<Type> sourceTypes;

        private boolean closed;

        private final OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory;

        /**
         * Instantiates a new Merge omni operator factory.
         *
         * @param operatorId the operator id
         * @param omniMergeId the omni merge id
         * @param sourceId the source id
         * @param exchangeClientSupplier the exchange client supplier
         * @param serdeFactory the serde factory
         * @param orderingCompiler the ordering compiler
         * @param types the types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         */
        public MergeOmniOperatorFactory(int operatorId, int omniMergeId, PlanNodeId sourceId,
            ExchangeClientSupplier exchangeClientSupplier, PagesSerdeFactory serdeFactory,
            OrderingCompiler orderingCompiler, List<Type> types, List<Integer> outputChannels,
            List<Integer> sortChannels, List<SortOrder> sortOrder) {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.sourceTypes= ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));          

            this.orderByOmniOperatorFactory = createOrderByOmniOperatorFactory(omniMergeId, sourceId, types,
                outputChannels, sortChannels, sortOrder);
        }

        @Override
        public PlanNodeId getSourceId() {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId,
                MergeOmniOperator.class.getSimpleName());

            return new MergeOmniOperator(operatorContext, sourceId, exchangeClientSupplier,
                serdeFactory.createPagesSerde(), orderByOmniOperatorFactory.createOperator());
        }

        @Override
        public void noMoreOperators() {
            closed = true;
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

    private final OperatorContext operatorContext;

    private final PlanNodeId sourceId;

    private final ExchangeClientSupplier exchangeClientSupplier;

    private final PagesSerde pagesSerde;

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    private final List<ExchangeClient> pageProducers = new ArrayList<>();

    private final Closer closer = Closer.create();

    private boolean closed;

    private final OrderByOmniOperator orderByOmniOperator;

    private boolean isFinished;

    /**
     * Instantiates a new Merge omni operator.
     *
     * @param operatorContext the operator context
     * @param sourceId the source id
     * @param exchangeClientSupplier the exchange client supplier
     * @param pagesSerde the pages serde
     * @param orderByOmniOperator the order by omni operator
     */
    public MergeOmniOperator(OperatorContext operatorContext, PlanNodeId sourceId,
        ExchangeClientSupplier exchangeClientSupplier, PagesSerde pagesSerde, Operator orderByOmniOperator) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");

        this.orderByOmniOperator = (OrderByOmniOperator) orderByOmniOperator;
    }

    @Override
    public PlanNodeId getSourceId() {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split) {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorSplit() instanceof RemoteSplit, "split is not a remote split");
        checkState(!blockedOnSplits.isDone(), "noMoreSplits has been called already");

        URI location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        ExchangeClient exchangeClient = closer.register(
            exchangeClientSupplier.get(operatorContext.localSystemMemoryContext(), pagesSerde));
        exchangeClient.addLocation(location);
        exchangeClient.setNoMoreLocation();

        pageProducers.add(exchangeClient);
        return Optional::empty;
    }

    @Override
    public void setNoMoreSplits() {
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public void finish() {
        close();
    }

    @Override
    public boolean isFinished() {
        return closed || orderByOmniOperator.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        if (!blockedOnSplits.isDone()) {
            return blockedOnSplits;
        }
        return orderByOmniOperator.isBlocked();
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput() {
        if (closed) {
            return null;
        }

        if (!isFinished && isSourceFinished()) {
            ImmutableList<List<Page>> pageCollections = pageProducers.stream()
                .map(ExchangeClient::getPages)
                .collect(toImmutableList());
            for (List<Page> pageList : pageCollections) {
                for (Page page : pageList) {
                    this.orderByOmniOperator.addInput(page);
                }
            }
            orderByOmniOperator.finish();
            isFinished = true;
        }

        Page page = orderByOmniOperator.getOutput();
        if (page != null) {
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public void close() {
        try {
            closer.close();
            closed = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isSourceFinished() {
        for (ExchangeClient exchangeClient : pageProducers) {
            if (exchangeClient.getPages() == null) {
                return false;
            }
        }
        return true;
    }
}
