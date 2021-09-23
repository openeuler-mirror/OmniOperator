/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.getVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;

import com.google.common.collect.Lists;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.project.MergePageStatus;
import io.prestosql.operator.project.MergePages;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.Iterator;
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

        VecBatch vecBatch = getVecBatch(page, getClass().getSimpleName());

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
            Iterator<Page> result = new VecBatchToPageIterator(new Iterator<VecBatch>() {
                private final VecBatch result = new VecBatch(createBlankVectors(vecTypes));

                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public VecBatch next() {
                    return result;
                }
            });
            finalPage = result.next();
            merge(getVecBatch(finalPage, getClass().getSimpleName()));
        }
        resetStatus();
        return finalPage;
    }

    /**
     * This method is used to merge the buffered vectors together
     * into a final result vector. It invokes append method defined natively
     * to perform merge operation.
     *
     * @param result Stores final resulting vectors
     */
    public void merge(VecBatch result) {
        int index = 0;
        for (Vec dest : result.getVectors()) {
            int offSet = 0;
            for (VecBatch batch : this.vecBatches) {
                Vec src = batch.getVectors()[index];

                int positionCount = src.getSize();
                if (src instanceof DictionaryVec) {
                    appendDictionaryValues(src, dest, offSet);
                } else {
                    dest.append(src, offSet, positionCount);
                }
                offSet += positionCount;
                src.close();
            }
            index++;
        }
    }

    private void appendDictionaryValues(Vec src, Vec dest, int offSet) {
        for (int index = 0; index < src.getSize(); index++) {
            VecType.VecTypeId id = ((DictionaryVec) src).getDictionary().getType().getId();
            switch (id) {
                case OMNI_VEC_TYPE_INT:
                    ((IntVec) dest).set(offSet + index, ((DictionaryVec) src).getInt(index));
                    break;
                case OMNI_VEC_TYPE_LONG:
                    ((LongVec) dest).set(offSet + index, ((DictionaryVec) src).getLong(index));
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    ((DoubleVec) dest).set(offSet + index, ((DictionaryVec) src).getDouble(index));
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    ((BooleanVec) dest).set(offSet + index, ((DictionaryVec) src).getBoolean(index));
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    ((VarcharVec) dest).set(offSet + index, ((DictionaryVec) src).getBytes(index));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + src.getType());
            }
        }
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

    /**
     * Create blank vectors list.
     *
     * @param vecTypes the vec types
     * @return the list
     */
    public List<Vec> createBlankVectors(VecType[] vecTypes) {
        List<Vec> vecsResult = new ArrayList<>();
        for (int i = 0; i < vecTypes.length; i++) {
            VecType type = vecTypes[i];
            switch (type.getId()) {
                case OMNI_VEC_TYPE_INT:
                case OMNI_VEC_TYPE_DATE32:
                    vecsResult.add(new IntVec(totalPositions));
                    break;
                case OMNI_VEC_TYPE_LONG:
                case OMNI_VEC_TYPE_DECIMAL64:
                    vecsResult.add(new LongVec(totalPositions));
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    vecsResult.add(new DoubleVec(totalPositions));
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    vecsResult.add(new BooleanVec(totalPositions));
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    int totalCapacity = 0;
                    for (VecBatch batch : this.vecBatches) {
                        Vec src = batch.getVectors()[i];
                        totalCapacity = totalCapacity + src.getCapacityInBytes();
                    }
                    vecsResult.add(new VarcharVec(totalCapacity, totalPositions));
                    break;
                case OMNI_VEC_TYPE_DECIMAL128:
                    vecsResult.add(new Decimal128Vec(totalPositions));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + type);
            }
        }
        return vecsResult;
    }
}
