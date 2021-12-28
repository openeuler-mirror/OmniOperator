/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.TestingSession.SESSION;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
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
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestDoubleArrayOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test(enabled = false)
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        DOUBLE.writeDouble(blockBuilder, 42.33);
        blockBuilder.appendNull();
        DOUBLE.writeDouble(blockBuilder, Double.MAX_VALUE);
        Block onHeapblock = blockBuilder.build();

        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, onHeapblock);

        assertTrue(block.isNull(0));
        assertEquals(DOUBLE.getDouble(block, 1), 42.33);
        assertTrue(block.isNull(2));
        assertEquals(DOUBLE.getDouble(block, 3), Double.MAX_VALUE);

        // build block from vec
        Block block1 = new DoubleArrayOmniBlock(4, (DoubleVec) block.getValues());
        assertTrue(block1.isNull(0));
        assertEquals(DOUBLE.getDouble(block1, 1), 42.33);
        assertTrue(block1.isNull(2));
        assertEquals(DOUBLE.getDouble(block1, 3), Double.MAX_VALUE);
        block.close();
    }

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlockByVec();
        DoubleArrayOmniBlock doubleArrayOmniBlock = new DoubleArrayOmniBlock(baseBlock.getPositionCount(),
            (DoubleVec) baseBlock.getValues());
        assertBlockEquals(DOUBLE, baseBlock, doubleArrayOmniBlock);
        assertEquals(baseBlock.toString(), doubleArrayOmniBlock.toString());

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        doubleArrayOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (size == ((DoubleVec) baseBlock.getValues()).getCapacityInBytes()) {
                isIdentical.set(true);
            }
        });
        assertTrue(isIdentical.get());

        Block regionOmniBlock = doubleArrayOmniBlock.getRegion(2, 2);
        assertEquals(regionOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionOmniBlock.getPositionCount(); i++) {
            assertEquals(regionOmniBlock.get(i), doubleArrayOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (DoubleVec) baseBlock.getValues());

        doubleArrayOmniBlock.close();
        regionOmniBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlockByVec();
        Block newBlock1 = new DoubleArrayOmniBlock(baseBlock.getPositionCount(), (DoubleVec) baseBlock.getValues());
        Block copyRegionBlock = newBlock1.copyRegion(0, newBlock1.getPositionCount());
        assertBlockEquals(copyRegionBlock, (DoubleVec) newBlock1.getValues());
        copyRegionBlock.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlockByVec();
        Block doubleArrayOmniBlock = new DoubleArrayOmniBlock(baseBlock.getPositionCount(),
            (DoubleVec) baseBlock.getValues());

        int[] positions = {0, 2, 3};
        Block copyRegionBlock = doubleArrayOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyRegionBlock.getDouble(i, 0), doubleArrayOmniBlock.getDouble(positions[i], 0));
        }

        doubleArrayOmniBlock.close();
        copyRegionBlock.close();
    }

    @Test
    public void testInvalidInput() {
        byte[] bytes = {};
        double[] values = {};
        assertThatThrownBy(
            () -> new DoubleArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, 1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(
            () -> new DoubleArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, -1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(
            () -> new DoubleArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        double[] values2len = new double[6];
        assertThatThrownBy(() -> new DoubleArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes,
            values2len)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("isNull length is less than positionCount");

        Block baseBlock = buildBlockByVec();
        DoubleVec expected = (DoubleVec) baseBlock.getValues();
        byte[] bytes2array = {};
        assertThatThrownBy(() -> new DoubleArrayOmniBlock(-1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(() -> new DoubleArrayOmniBlock(1, -1, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(() -> new DoubleArrayOmniBlock(1, 6, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        assertThatThrownBy(() -> new DoubleArrayOmniBlock(1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        Block baseBlock = buildBlockByVec();
        Block doubleArrayOmniBlock = new DoubleArrayOmniBlock(baseBlock.getPositionCount(),
            (DoubleVec) baseBlock.getValues());
        long expect = 9;
        long expectSizeBytes = 36;
        long expectStates = 8;
        boolean[] position = {true, true, true, true};
        assertEquals(doubleArrayOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(doubleArrayOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(doubleArrayOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(doubleArrayOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        doubleArrayOmniBlock.close();
    }

    private Block buildBlockByVec() {
        DoubleVec doubleVec = new DoubleVec(4);
        doubleVec.set(0, 42.33);
        doubleVec.set(0, 43.34);
        doubleVec.set(0, 44.35);
        doubleVec.set(0, 45.36);

        return new DoubleArrayOmniBlock(4, doubleVec);
    }

    private static void assertBlockEquals(Block actual, DoubleVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals((Double) actual.get(position), new Double(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
