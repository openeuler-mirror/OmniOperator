/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.WorkProcessor.ProcessState.finished;
import static io.prestosql.operator.WorkProcessor.ProcessState.ofResult;
import static io.prestosql.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_NATIVE_ERROR;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.SelectedPositions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.sql.gen.ExpressionProfiler;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalInt;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The type Omni page processor.
 *
 * @since 20210630
 */
@NotThreadSafe
public class OmniPageProcessor extends PageProcessor {
    private final ExpressionProfiler expressionProfiler;

    private final OmniProjection projection;

    private final VecAllocator vecAllocator;

    private final int projectBatchSize;

    private Optional<OmniPageFilter.OmniPageFilterOperator> omniPageFilterOperator = Optional.empty();

    /**
     * Instantiates a new Omni page processor.
     *
     * @param filter the filter
     * @param proj the proj
     * @param initialBatchSize the initial batch size
     * @param expressionProfiler the expression profiler
     */
    public OmniPageProcessor(VecAllocator vecAllocator, Optional<PageFilter> filter, OmniProjection proj, OptionalInt initialBatchSize,
        ExpressionProfiler expressionProfiler) {
        super(filter, initialBatchSize, expressionProfiler);
        this.vecAllocator = vecAllocator;
        if (filter.isPresent()) {
            PageFilter pageFilter = filter.get();
            this.omniPageFilterOperator = Optional.of(((OmniPageFilter) pageFilter).getOperator(vecAllocator));
        }
        this.projection = requireNonNull(proj, "projection is null");
        this.projectBatchSize = initialBatchSize.orElse(1);
        this.expressionProfiler = requireNonNull(expressionProfiler, "expressionProfiler is null");
    }

    public OmniProjection getProjection() {
        return this.projection;
    }

    @Override
    public WorkProcessor<Page> createWorkProcessor(ConnectorSession session, DriverYieldSignal yieldSignal,
        LocalMemoryContext memoryContext, Page page) {
        if (page.getPositionCount() == 0) {
            return WorkProcessor.of();
        }
        Page toProject = page;
        if (omniPageFilterOperator.isPresent()) {
            Page filterAndProjectPage = omniPageFilterOperator.get().filterWithProject(vecAllocator, session, page);
            if (filterAndProjectPage == null) {
                return WorkProcessor.of();
            }
            // Filtered rows have already been made into a page by filterWithProject
            toProject = filterAndProjectPage;
        }
        int[] neededCols = projection.getNeededCols();
        // Check for special case where excess columns are returned from nested query
        if (toProject.getBlocks().length != neededCols.length) {
            Block[] newBlocks = new Block[neededCols.length];
            for (int i = 0; i < newBlocks.length; i++) {
                newBlocks[i] = toProject.getBlock(neededCols[i]);
            }
            toProject = new Page(newBlocks);
        }
        return WorkProcessor.create(new OmniProjectSelectedPositions(vecAllocator, session, yieldSignal, memoryContext, toProject,
            positionsRange(0, toProject.getPositionCount())));
    }

    private class OmniProjectSelectedPositions implements WorkProcessor.Process<Page> {
        private final VecAllocator vecAllocator;

        private final ConnectorSession session;

        private final DriverYieldSignal yieldSignal;

        private final LocalMemoryContext memoryContext;

        private final Page page;

        private final SelectedPositions selectedPositions;

        private boolean isFinished;

        /**
         * Instantiates a new Omni project selected positions.
         *
         * @param vecAllocator vector allocator
         * @param session the session
         * @param yieldSignal the yield signal
         * @param memoryContext the memory context
         * @param page the page
         * @param selectedPositions the selected positions
         */
        public OmniProjectSelectedPositions(VecAllocator vecAllocator, ConnectorSession session, DriverYieldSignal yieldSignal,
                LocalMemoryContext memoryContext, Page page, SelectedPositions selectedPositions) {
            this.vecAllocator = vecAllocator;
            this.session = session;
            this.yieldSignal = yieldSignal;
            this.memoryContext = memoryContext;
            this.page = page;
            this.selectedPositions = selectedPositions;
            this.isFinished = false;
        }

        @Override
        public ProcessState<Page> process() {
            if (isFinished) {
                return finished();
            }
            OmniOperator operator = projection.getFactory().createOperator(vecAllocator);

            VecBatch vecBatch = buildVecBatch(vecAllocator, page, getClass().getSimpleName());
            operator.addInput(vecBatch);
            Iterator<Page> result = new VecBatchToPageIterator(operator.getOutput());
            if (!result.hasNext()) {
                throw new OmniRuntimeException(OMNI_NATIVE_ERROR, "Filter returns empty result");
            }
            Page projectedPage = result.next();
            isFinished = true;
            page.close();
            return ofResult(projectedPage);
        }
    }
}