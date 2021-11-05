/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.project.MergePages;
import io.prestosql.operator.project.MergingPageOutput;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

public class FilterAndProjectOmniOperator implements Operator {
    private final OperatorContext operatorContext;

    private final LocalMemoryContext pageProcessorMemoryContext;

    private final LocalMemoryContext outputMemoryContext;

    private final PageProcessor processor;

    private final OmniMergingPageOutput mergingOutput;

    private boolean finishing;

    public FilterAndProjectOmniOperator(OperatorContext operatorContext, PageProcessor processor,
                                        OmniMergingPageOutput mergingOutput) {
        this.processor = requireNonNull(processor, "processor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageProcessorMemoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(
            FilterAndProjectOmniOperator.class.getSimpleName());
        this.outputMemoryContext = operatorContext.newLocalSystemMemoryContext(
            FilterAndProjectOmniOperator.class.getSimpleName());
        this.mergingOutput = requireNonNull(mergingOutput, "mergingOutput is null");
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public final void finish() {
        mergingOutput.finish();
        finishing = true;
    }

    @Override
    public final boolean isFinished() {
        boolean finished = finishing && mergingOutput.isFinished();
        if (finished) {
            outputMemoryContext.setBytes(mergingOutput.getRetainedSizeInBytes());
        }
        return finished;
    }

    @Override
    public final boolean needsInput() {
        return !finishing && mergingOutput.needsInput();
    }

    @Override
    public final void addInput(Page page) {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(mergingOutput.needsInput(), "Page buffer is full");

        mergingOutput.addInput(processor.process(operatorContext.getSession().toConnectorSession(),
            operatorContext.getDriverContext().getYieldSignal(), pageProcessorMemoryContext, page));
        outputMemoryContext.setBytes(mergingOutput.getRetainedSizeInBytes() + pageProcessorMemoryContext.getBytes());
    }

    @Override
    public final Page getOutput() {
        return mergingOutput.getOutput();
    }

    public static class FilterAndProjectOmniOperatorFactory implements OperatorFactory {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final Supplier<PageProcessor> processor;

        private final List<Type> types;

        private final DataSize minOutputPageSize;

        private final int minOutputPageRowCount;

        private boolean closed;

        private static Constructor<?> mergingPageConstructor = null;

        private final Session session;

        public FilterAndProjectOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                                   Supplier<PageProcessor> processor, List<Type> types,
                                                   DataSize minOutputPageSize, int minOutputPageRowCount) {
            this(operatorId, planNodeId, processor, types, minOutputPageSize, minOutputPageRowCount, null);
        }

        public FilterAndProjectOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                                   Supplier<PageProcessor> processor, List<Type> types,
                                                   DataSize minOutputPageSize, int minOutputPageRowCount,
                                                   Session session) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.session = session;
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                FilterAndProjectOmniOperator.class.getSimpleName());
            return new FilterAndProjectOmniOperator(operatorContext, processor.get(),
                new OmniMergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount));
        }

        @Override
        public void noMoreOperators() {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            return new FilterAndProjectOmniOperatorFactory(operatorId, planNodeId, processor, types, minOutputPageSize,
                minOutputPageRowCount, session);
        }
    }

    // FIXME: using {$link OmniMergingPageOutput} if its performance is reach requirement.
    public static class MockMergingPageOutput extends MergingPageOutput {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(MockMergingPageOutput.class).instanceSize();

        private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

        private final List<Type> types;

        private final PageBuilder pageBuilder;

        private final Queue<Page> outputQueue = new LinkedList<>();

        private final long minPageSizeInBytes;

        private final int minRowCount;

        @Nullable
        private Iterator<Optional<Page>> currentInput;

        private boolean finishing;

        public MockMergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount) {
            this(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        }

        public MockMergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount,
                                     int maxPageSizeInBytes) {
            super(types, minPageSizeInBytes, minRowCount, maxPageSizeInBytes);
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
            checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
            checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
            checkArgument(maxPageSizeInBytes >= minPageSizeInBytes,
                "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
            checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %s",
                MAX_MIN_PAGE_SIZE);
            this.minPageSizeInBytes = minPageSizeInBytes;
            this.minRowCount = minRowCount;
            pageBuilder = PageBuilder.withMaxPageSize(maxPageSizeInBytes, this.types);
        }

        public boolean needsInput() {
            return currentInput == null && !finishing && outputQueue.isEmpty();
        }

        public void addInput(Iterator<Optional<Page>> input) {
            requireNonNull(input, "input is null");
            checkState(!finishing, "output is in finishing state");
            checkState(currentInput == null, "currentInput is present");
            currentInput = input;
        }

        @Nullable
        public Page getOutput() {
            if (!outputQueue.isEmpty()) {
                return outputQueue.poll();
            }

            while (currentInput != null) {
                if (!currentInput.hasNext()) {
                    currentInput = null;
                    break;
                }

                if (!outputQueue.isEmpty()) {
                    break;
                }

                Optional<Page> next = currentInput.next();
                if (next.isPresent()) {
                    process(next.get());
                } else {
                    break;
                }
            }

            if (currentInput == null && finishing) {
                flush();
            }

            return outputQueue.poll();
        }

        public void finish() {
            finishing = true;
        }

        public boolean isFinished() {
            return finishing && currentInput == null && outputQueue.isEmpty() && pageBuilder.isEmpty();
        }

        private void process(Page page) {
            requireNonNull(page, "page is null");

            // avoid memory copying for pages that are big enough
            if (page.getPositionCount() >= minRowCount || page.getSizeInBytes() >= minPageSizeInBytes) {
                flush();
                outputQueue.add(page);
                return;
            }

            buffer(page);
        }

        private void buffer(Page page) {
            pageBuilder.declarePositions(page.getPositionCount());
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                }
            }
            // here to close off heap resource.
            page.close();
            if (pageBuilder.isFull()) {
                flush();
            }
        }

        private void flush() {
            if (!pageBuilder.isEmpty()) {
                Page output = pageBuilder.build();
                pageBuilder.reset();
                outputQueue.add(output);
            }
        }

        public long getRetainedSizeInBytes() {
            long retainedSizeInBytes = INSTANCE_SIZE;
            retainedSizeInBytes += pageBuilder.getRetainedSizeInBytes();
            for (Page page : outputQueue) {
                retainedSizeInBytes += page.getRetainedSizeInBytes();
            }
            return retainedSizeInBytes;
        }
    }
}
