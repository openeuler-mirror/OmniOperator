/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestLongArrayOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, Long.MAX_VALUE);
        Block onHeapBlock = blockBuilder.build();
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, onHeapBlock);

        assertTrue(block.isNull(0));
        assertEquals(BIGINT.getLong(block, 1), 42L);
        assertTrue(block.isNull(2));
        assertEquals(BIGINT.getLong(block, 3), Long.MAX_VALUE);

        // build block from vec
        Block longArrayOmniBlock = new LongArrayOmniBlock(4, (LongVec) block.getValues());
        assertTrue(longArrayOmniBlock.isNull(0));
        assertEquals(BIGINT.getLong(longArrayOmniBlock, 1), 42L);
        assertTrue(longArrayOmniBlock.isNull(2));
        assertEquals(BIGINT.getLong(longArrayOmniBlock, 3), Long.MAX_VALUE);

        block.close();
    }

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlockByBuilder();
        LongArrayOmniBlock longArrayOmniBlock = new LongArrayOmniBlock(baseBlock.getPositionCount(),
            (LongVec) baseBlock.getValues());
        assertBlockEquals(BIGINT, longArrayOmniBlock, baseBlock);
        assertEquals(baseBlock.toString(), longArrayOmniBlock.toString());

        Block regionOmniBlock = longArrayOmniBlock.getRegion(2, 2);
        assertEquals(regionOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionOmniBlock.getPositionCount(); i++) {
            assertEquals(regionOmniBlock.get(i), longArrayOmniBlock.get(i + 2));
        }

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        longArrayOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (size == ((LongVec) baseBlock.getValues()).getCapacityInBytes()) {
                isIdentical.set(true);
            }
        });
        Assert.assertTrue(isIdentical.get());

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (LongVec) baseBlock.getValues());

        longArrayOmniBlock.close();
        regionOmniBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlockByBuilder();
        Block longArrayOmniBlock = new LongArrayOmniBlock(baseBlock.getPositionCount(),
            (LongVec) baseBlock.getValues());
        Block copyRegionBlock = longArrayOmniBlock.copyRegion(0, longArrayOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock, (LongVec) longArrayOmniBlock.getValues());

        Block copyNotEqualRegionBlock = longArrayOmniBlock.copyRegion(0, 3);
        assertBlockEquals(copyNotEqualRegionBlock, (LongVec) longArrayOmniBlock.getValues());

        copyNotEqualRegionBlock.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlockByBuilder();
        Block longArrayOmniBlock = new LongArrayOmniBlock(baseBlock.getPositionCount(),
            (LongVec) baseBlock.getValues());

        int[] positions = {0, 2, 3};
        Block copyRegionBlock = longArrayOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyRegionBlock.getLong(i, 0), longArrayOmniBlock.getLong(positions[i], 0));
        }

        longArrayOmniBlock.close();
        copyRegionBlock.close();
    }

    @Test
    public void testInvalidInput() {
        Block baseBlock = buildBlockByBuilder();
        byte[] bytes = {};
        long[] values = {};
        assertThatThrownBy(
            () -> new LongArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, 1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(
            () -> new LongArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, -1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(
            () -> new LongArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        long[] values2len = new long[6];
        assertThatThrownBy(() -> new LongArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes,
            values2len)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("isNull length is less than positionCount");

        LongVec expected = (LongVec) baseBlock.getValues();
        byte[] bytes2array = {};
        assertThatThrownBy(() -> new LongArrayOmniBlock(-1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(() -> new LongArrayOmniBlock(1, -1, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(() -> new LongArrayOmniBlock(1, 6, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        assertThatThrownBy(() -> new LongArrayOmniBlock(1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        Block baseBlock = buildBlockByBuilder();
        Block longArrayOmniBlock = new LongArrayOmniBlock(baseBlock.getPositionCount(),
            (LongVec) baseBlock.getValues());
        long expect = 9;
        long expectSizeBytes = 36;
        long expectStates = 8;
        boolean[] position = {true, true, true, true};
        assertEquals(longArrayOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(longArrayOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(longArrayOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(longArrayOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        longArrayOmniBlock.close();
    }

    private Block buildBlockByBuilder() {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 4);
        BIGINT.writeLong(blockBuilder, 42);
        BIGINT.writeLong(blockBuilder, 43);
        BIGINT.writeLong(blockBuilder, 44);
        BIGINT.writeLong(blockBuilder, 45);
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());
    }

    private static void assertBlockEquals(Block actual, LongVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals((Long) actual.get(position), new Long(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
