/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.createBlankVectors;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;
import static nova.hetu.olk.tool.OperatorUtils.merge;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromBlocks;
import static nova.hetu.omniruntime.type.VecType.VecTypeId.OMNI_VEC_TYPE_VARCHAR;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.project.MergePageStatus;
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
    private long currentPageSizeInBytes;

    private long retainedSizeInBytes;

    private int totalPositions;

    private final VecType[] vecTypes;

    private Page lastInputPage;

    /**
     * The Vec batches.
     */
    List<VecBatch> vecBatches;

    private MergePageStatus status;

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
        this.vecBatches = new ArrayList<>();
        this.vecTypes = toVecTypes(inputTypes);
        this.status = MergePageStatus.EMPTY;
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public MergePageStatus getStatus() {
        return status;
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
            if (getStatus() == MergePageStatus.EMPTY) {
                memoryContext.close();
                return finished();
            }

            return ofResult(getOutput(), false);
        }

        if (inputPage.getPositionCount() >= minRowCount || inputPage.getSizeInBytes() >= minPageSizeInBytes) {
            if (getStatus() == MergePageStatus.EMPTY) {
                return ofResult(inputPage);
            }

            Page output = getOutput();
            // inputPage is preserved until next process(...) call
            queuedPage = inputPage;
            memoryContext.setBytes(getRetainedSizeInBytes() + inputPage.getRetainedSizeInBytes());
            return ofResult(output, false);
        }

        appendPage(inputPage);

        if (getStatus() == MergePageStatus.FULL) {
            return ofResult(getOutput());
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
        lastInputPage = page;

        VecAllocator vecAllocator = getVecAllocatorFromBlocks(page.getBlocks());
        VecBatch vecBatch = buildVecBatch(vecAllocator, page, getClass().getSimpleName());

        totalPositions += page.getPositionCount();
        updatePageSize(page);
        vecBatches.add(vecBatch);

        if (isFull()) {
            this.status = MergePageStatus.FULL;
        } else {
            this.status = MergePageStatus.MORE_DATA;
        }
    }

    /**
     * Update page size.
     *
     * @param page the page
     */
    public void updatePageSize(Page page) {
        currentPageSizeInBytes = currentPageSizeInBytes + page.getSizeInBytes();
        retainedSizeInBytes = retainedSizeInBytes + page.getRetainedSizeInBytes();
    }

    /**
     * Is full boolean.
     *
     * @return the boolean
     */
    public boolean isFull() {
        return currentPageSizeInBytes >= maxPageSizeInBytes || totalPositions == Integer.MAX_VALUE;
    }

    /**
     * This method is invoked to retrieve the final merged page.
     *
     * @return Page output
     */
    public Page getOutput() {
        Page finalPage;
        if (vecBatches.isEmpty()) {
            finalPage = new Page(lastInputPage.getPositionCount());
        } else {
            // Merge buffered vectors
            int[] varcharCapacities = new int[vecTypes.length];
            for (int channel = 0; channel < vecTypes.length; channel++) {
                if (vecTypes[channel].getId() == OMNI_VEC_TYPE_VARCHAR) {
                    for (VecBatch batch : this.vecBatches) {
                        Vec src = batch.getVectors()[channel];
                        varcharCapacities[channel] = varcharCapacities[channel] + src.getCapacityInBytes();
                    }
                }
            }

            VecBatch mergeResult = new VecBatch(createBlankVectors(getVecAllocatorFromBlocks(lastInputPage.getBlocks()), vecTypes, totalPositions, varcharCapacities));
            merge(mergeResult, this.vecBatches);
            finalPage = new VecBatchToPageIterator(ImmutableList.of(mergeResult).iterator()).next();
        }
        resetStatus();
        return finalPage;
    }

    /**
     * Reset status.
     */
    public void resetStatus() {
        currentPageSizeInBytes = 0;
        retainedSizeInBytes = 0;
        totalPositions = 0;
        vecBatches.clear();
        this.status = MergePageStatus.EMPTY;
    }
}
