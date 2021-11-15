/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LazyBlock;
import java.util.function.BiConsumer;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.LazyVec;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * Lazy block, support lazy to load data when necessary.
 *
 * @param <T> Block type
 */
public class LazyOmniBlock<T> implements Block<T> {
    private LazyBlock lazyBlock;

    private final LazyVec nativeLazyVec;

    public LazyOmniBlock(VecAllocator vecAllocator, LazyBlock lazyBlock) {
        this.lazyBlock = lazyBlock;
        nativeLazyVec = new LazyVec(vecAllocator, lazyBlock.getPositionCount(), () -> {
            Block block = lazyBlock.getLoadedBlock();
            return OperatorUtils.buildVec(vecAllocator, block);
        });
    }

    @Override
    public boolean isExtensionBlock() {
        return true;
    }

    @Override
    public Object getValues() {
        return nativeLazyVec;
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        lazyBlock.writePositionTo(position, blockBuilder);
    }

    @Override
    public Block<T> getSingleValueBlock(int position) {
        return lazyBlock.getSingleValueBlock(position);
    }

    @Override
    public int getPositionCount() {
        return lazyBlock.getPositionCount();
    }

    @Override
    public long getSizeInBytes() {
        return lazyBlock.getSizeInBytes();
    }

    @Override
    public long getRegionSizeInBytes(int position, int length) {
        return lazyBlock.getRegionSizeInBytes(position, length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions) {
        return lazyBlock.getPositionsSizeInBytes(positions);
    }

    @Override
    public long getRetainedSizeInBytes() {
        return lazyBlock.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position) {
        return lazyBlock.getEstimatedDataSizeForStats(position);
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer) {
        lazyBlock.retainedBytesForEachPart(consumer);
    }

    @Override
    public String getEncodingName() {
        return lazyBlock.getEncodingName();
    }

    @Override
    public Block<T> copyPositions(int[] positions, int offset, int length) {
        return lazyBlock.copyPositions(positions, offset, length);
    }

    @Override
    public Block<T> getRegion(int positionOffset, int length) {
        return lazyBlock.getRegion(positionOffset, length);
    }

    @Override
    public Block<T> copyRegion(int position, int length) {
        return lazyBlock.copyRegion(position, length);
    }

    @Override
    public boolean isNull(int position) {
        return lazyBlock.isNull(position);
    }

    @Override
    public Block<T> getLoadedBlock() {
        return lazyBlock.getLoadedBlock();
    }
}
