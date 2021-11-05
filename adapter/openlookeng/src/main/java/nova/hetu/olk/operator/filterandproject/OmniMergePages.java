/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createBlankVectors;
import static nova.hetu.olk.tool.OperatorUtils.merge;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromBlocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.project.MergePages;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Omni merge pages.
 *
 * @since 20210630
 */
public class OmniMergePages extends MergePages.MergePagesTransformation {
    private final VecType[] vecTypes;

    List<Page> pages;

    private long currentPageSizeInBytes;

    private long retainedSizeInBytes;

    private int totalPositions;

    private VecAllocator vecAllocator;

    /**
     * Instantiates a new Omni merge pages.
     *
     * @param types the types
     * @param minPageSizeInBytes the min page size in bytes
     * @param minRowCount the min row count
     * @param maxPageSizeInBytes the max page size in bytes
     * @param memoryContext the memory context
     */
    public OmniMergePages(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount,
        int maxPageSizeInBytes, LocalMemoryContext memoryContext) {
        super(types, minPageSizeInBytes, minRowCount, maxPageSizeInBytes, memoryContext);
        List<Type> inputTypes = Lists.newArrayList(requireNonNull(types, "types is null"));
        this.pages = new ArrayList<>();
        this.vecTypes = toVecTypes(inputTypes);
    }

    /**
     * Gets retained size in bytes.
     *
     * @return the retained size in bytes
     */
    public long getRetainedSizeInBytes() {
        return retainedSizeInBytes;
    }

    @Override
    public WorkProcessor.TransformationState<Page> processPage(Page inputPage) {
        if (queuedPage != null) {
            Page output = queuedPage;
            queuedPage = null;
            memoryContext.setBytes(getRetainedSizeInBytes());
            return ofResult(output);
        }

        boolean inputFinished = inputPage == null;
        if (inputFinished) {
            if (pages.isEmpty()) {
                memoryContext.close();
                return finished();
            }

            return ofResult(flush(), false);
        }

        if (inputPage.getPositionCount() >= minRowCount || inputPage.getSizeInBytes() >= minPageSizeInBytes) {
            if (pages.isEmpty()) {
                return ofResult(inputPage);
            }

            Page output = flush();
            // inputPage is preserved until next process(...) call
            queuedPage = inputPage;
            memoryContext.setBytes(getRetainedSizeInBytes() + inputPage.getRetainedSizeInBytes());
            return ofResult(output, false);
        }

        appendPage(inputPage);

        if (isFull()) {
            return ofResult(flush());
        }

        memoryContext.setBytes(getRetainedSizeInBytes());
        return needsMoreData();
    }

    /**
     * This method takes in one page at a time and buffers it in a
     * list so that the final merged page can be created.
     *
     * @param page A Page to be merged.
     */
    public void appendPage(Page page) {
        // VecAllocator is only created once
        if (this.vecAllocator == null) {
            this.vecAllocator = getVecAllocatorFromBlocks(page.getBlocks());
        }
        pages.add(page);
        totalPositions += page.getPositionCount();
        currentPageSizeInBytes = currentPageSizeInBytes + page.getSizeInBytes();
        retainedSizeInBytes = retainedSizeInBytes + page.getRetainedSizeInBytes();
    }

    /**
     * Is full boolean.
     *
     * @return the boolean
     */
    public boolean isFull() {
        return totalPositions == Integer.MAX_VALUE || currentPageSizeInBytes >= maxPageSizeInBytes;
    }

    /**
     * This method is invoked to retrieve the final merged page.
     *
     * @return Page output
     */
    public Page flush() {
        VecBatch mergeResult = new VecBatch(createBlankVectors(vecAllocator, vecTypes, totalPositions));
        merge(mergeResult, pages, vecAllocator);
        Page finalPage = new VecBatchToPageIterator(ImmutableList.of(mergeResult).iterator()).next();
        currentPageSizeInBytes = 0;
        retainedSizeInBytes = 0;
        totalPositions = 0;
        pages.clear();
        return finalPage;
    }
}
