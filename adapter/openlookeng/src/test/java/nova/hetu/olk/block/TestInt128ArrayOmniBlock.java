/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;

import io.prestosql.spi.block.BlockEncodingSerde;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestInt128ArrayOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testMultipleValuesWithNull() {
        int positionCount = 4;
        long[] values = {0L, 0L, 0L, 42L, 0L, 0L, Long.MAX_VALUE, Long.MAX_VALUE};
        byte[] valueIsNull = {Vec.NULL, Vec.NOT_NULL, Vec.NULL, Vec.NOT_NULL};
        Int128ArrayOmniBlock block = new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, positionCount,
            Optional.of(valueIsNull), values);

        assertTrue(block.isNull(0));
        assertEquals(block.get(1), new long[] {0L, 42L});
        assertTrue(block.isNull(2));
        assertEquals(block.get(3), new long[] {Long.MAX_VALUE, Long.MAX_VALUE});

        // build block from vec
        Block block1 = new Int128ArrayOmniBlock(4, (Decimal128Vec) block.getValues());
        assertTrue(block1.isNull(0));
        assertEquals(block1.get(1), new long[] {0L, 42L});
        assertTrue(block1.isNull(2));
        assertEquals(block.get(3), new long[] {Long.MAX_VALUE, Long.MAX_VALUE});
        block.close();
    }

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlock();
        Int128ArrayOmniBlock int128ArrayOmniBlock = new Int128ArrayOmniBlock(baseBlock.getPositionCount(),
            (Decimal128Vec) baseBlock.getValues());
        assertBlockEquals(int128ArrayOmniBlock, baseBlock);
        String expect = "Int128ArrayOmniBlock{positionCount=4}";
        assertEquals(int128ArrayOmniBlock.toString(), expect);

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        int128ArrayOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (part == baseBlock.getValues()) {
                isIdentical.set(true);
            }
        });
        Assert.assertTrue(isIdentical.get());

        Block regionInt128ArrayOmniBlock = int128ArrayOmniBlock.getRegion(2, 2);
        assertEquals(regionInt128ArrayOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionInt128ArrayOmniBlock.getPositionCount(); i++) {
            assertEquals(regionInt128ArrayOmniBlock.get(i), int128ArrayOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, baseBlock);

        int128ArrayOmniBlock.close();
        regionInt128ArrayOmniBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlock();
        Block int128ArrayOmniBlock = new Int128ArrayOmniBlock(baseBlock.getPositionCount(),
            (Decimal128Vec) baseBlock.getValues());
        Block copyRegionBlock = int128ArrayOmniBlock.copyRegion(0, int128ArrayOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock, int128ArrayOmniBlock);

        copyRegionBlock.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlock();
        Block int128ArrayOmniBlock = new Int128ArrayOmniBlock(baseBlock.getPositionCount(),
            (Decimal128Vec) baseBlock.getValues());

        int[] positions = {0, 1, 2, 3};
        Block copyRegionBlock = int128ArrayOmniBlock.copyPositions(positions, 0, 4);
        assertBlockEquals(copyRegionBlock, int128ArrayOmniBlock);

        int128ArrayOmniBlock.close();
        copyRegionBlock.close();
    }

    @Test
    public void testInvalidInput() {
        byte[] bytes = {};
        long[] values = {};
        assertThatThrownBy(
            () -> new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, 1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionOffset is negative");

        assertThatThrownBy(
            () -> new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, -1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(
            () -> new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        long[] values2len = new long[6];
        assertThatThrownBy(() -> new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 1, bytes,
            values2len)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("isNull length is less than positionCount");

        Block baseBlock = buildBlock();
        Decimal128Vec expected = (Decimal128Vec) baseBlock.getValues();
        byte[] bytes2array = {};
        assertThatThrownBy(() -> new Int128ArrayOmniBlock(-1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionOffset is negative");

        assertThatThrownBy(() -> new Int128ArrayOmniBlock(1, -1, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(() -> new Int128ArrayOmniBlock(1, 6, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        assertThatThrownBy(() -> new Int128ArrayOmniBlock(1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        Block baseBlock = buildBlock();
        Block int128ArrayOmniBlock = new Int128ArrayOmniBlock(baseBlock.getPositionCount(),
            (Decimal128Vec) baseBlock.getValues());
        long expect = 17;
        long expectSizeBytes = 68;
        long expectStates = 0;
        boolean[] position = {true, true, true, true};
        assertEquals(int128ArrayOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(int128ArrayOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(int128ArrayOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(int128ArrayOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        int128ArrayOmniBlock.close();
    }

    private Block buildBlock() {
        int positionCount = 4;
        long[] values = {0L, 0L, 0L, 42L, 0L, 0L, Long.MAX_VALUE, Long.MAX_VALUE};
        byte[] valueIsNull = {Vec.NULL, Vec.NOT_NULL, Vec.NULL, Vec.NOT_NULL};
        Int128ArrayOmniBlock block = new Int128ArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, positionCount,
            Optional.of(valueIsNull), values);
        return block;
    }

    private static void assertBlockEquals(Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(actual.get(position), expected.get(position));
        }
    }
}
