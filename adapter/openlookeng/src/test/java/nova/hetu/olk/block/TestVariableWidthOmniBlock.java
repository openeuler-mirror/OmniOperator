/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVariableWidthOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlockByBuilder();
        VariableWidthOmniBlock variableWidthOmniBlock = new VariableWidthOmniBlock(baseBlock.getPositionCount(),
            (VarcharVec) baseBlock.getValues());
        assertBlockEquals(VARCHAR, variableWidthOmniBlock, baseBlock);
        assertEquals(baseBlock.toString(), variableWidthOmniBlock.toString());

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        variableWidthOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (size == ((VarcharVec) baseBlock.getValues()).getCapacityInBytes()) {
                isIdentical.set(true);
            }
        });
        assertTrue(isIdentical.get());

        variableWidthOmniBlock.setClosable(true);

        Block regionOmniBlock = variableWidthOmniBlock.getRegion(2, 2);
        assertEquals(regionOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionOmniBlock.getPositionCount(); i++) {
            assertEquals(regionOmniBlock.get(i), variableWidthOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (VarcharVec) baseBlock.getValues());

        variableWidthOmniBlock.close();
        regionOmniBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlockByBuilder();
        Block variableWidthOmniBlock = new VariableWidthOmniBlock(baseBlock.getPositionCount(),
            (VarcharVec) baseBlock.getValues());
        Block copyRegionBlock = variableWidthOmniBlock.copyRegion(0, variableWidthOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock, (VarcharVec) variableWidthOmniBlock.getValues());

        Block copyNotEqualRegionBlock = variableWidthOmniBlock.copyRegion(0, 3);
        assertBlockEquals(copyNotEqualRegionBlock, (VarcharVec) variableWidthOmniBlock.getValues());

        copyNotEqualRegionBlock.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlockByBuilder();
        Block variableWidthOmniBlock = new VariableWidthOmniBlock(baseBlock.getPositionCount(),
            (VarcharVec) baseBlock.getValues());

        int[] positions = {0, 2, 3};
        Block copyRegionBlock = variableWidthOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyRegionBlock.getString(i, 0, 0), variableWidthOmniBlock.getString(positions[i], 0, 0));
        }
        variableWidthOmniBlock.close();
        copyRegionBlock.close();
    }

    @Test
    public void testVarcharVecWithLastValueIsNull() {
        int position = 5;
        String[] strs = new String[] {"alice", "bob", "charlie"};
        StringBuilder builder = new StringBuilder();
        for (String data : strs) {
            builder.append(data);
        }
        int[] offset = new int[] {0, 5, 8, 15};
        VarcharVec values = new VarcharVec(1024, position);
        values.put(0, builder.toString().getBytes(StandardCharsets.UTF_8), 0, offset, 0, 3);
        values.setNull(3);
        values.setNull(4);
        VariableWidthOmniBlock block = new VariableWidthOmniBlock(position, values);
        int totalLen = 0;
        for (int i = 0; i < position; i++) {
            totalLen += block.getSliceLength(i);
        }
        assertEquals(totalLen, 15);
        totalLen = 0;
        VariableWidthOmniBlock variableWidthOmniBlock = new VariableWidthOmniBlock(3, values.slice(2, 5));
        for (int i = 0; i < 3; i++) {
            totalLen += variableWidthOmniBlock.getSliceLength(i);
        }
        assertEquals(totalLen, 7);

        block.close();
        variableWidthOmniBlock.close();
    }

    @Test
    public void testFilter() {
        int count = 1024;
        int size = 1000;
        boolean[] valid = new boolean[count];
        Arrays.fill(valid, Boolean.TRUE);
        VariableWidthOmniBlock block = getBlock(count);
        String[] values = new String[block.getPositionCount()];

        BloomFilter bf = getBf(size);
        for (int i = 0; i < block.getPositionCount(); i++) {
            values[i] = block.getString(i, 0, 0);
        }

        boolean[] actualValidPositions = block.filter(bf, valid);
        assertEquals(actualValidPositions, valid);

        int[] positions = {0, 1, 2, 3};
        int positionCount = 4;
        int[] matchedPosition = new int[4];
        int actualFilterPositions = block.filter(positions, positionCount, matchedPosition, (x) -> {
            return true;
        });
        assertEquals(actualFilterPositions, positionCount);
        block.close();
    }

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "alice");
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "bob");
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());

        assertTrue(block.isNull(0));
        assertEquals(VARCHAR.getObjectValue(SESSION, block, 1), "alice");
        assertTrue(block.isNull(2));
        assertEquals(VARCHAR.getObjectValue(SESSION, block, 3), "bob");

        // build block from vec
        Block block1 = new VariableWidthOmniBlock(4, (VarcharVec) block.getValues());
        assertTrue(block1.isNull(0));
        assertEquals(VARCHAR.getObjectValue(SESSION, block1, 1), "alice");
        assertTrue(block1.isNull(2));
        assertEquals(VARCHAR.getObjectValue(SESSION, block1, 3), "bob");
        block.close();
    }

    @Test
    public void testInvalidInput() {
        Block baseBlock = buildBlockByBuilder();
        int[] offsets = {};
        byte[] bytes = {};
        assertThatThrownBy(
            () -> new VariableWidthOmniBlock(-1, baseBlock.getPositionCount(), (VarcharVec) baseBlock.getValues(),
                offsets, bytes)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("arrayOffset is negative");

        int[] offsets2 = {};
        byte[] bytes2 = {};
        assertThatThrownBy(() -> new VariableWidthOmniBlock(1, -1, (VarcharVec) baseBlock.getValues(), offsets2,
            bytes2)).isInstanceOfAny(IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        int[] offsets2values = {};
        byte[] bytes2values = {};
        assertThatThrownBy(() -> new VariableWidthOmniBlock(1, 2, null, offsets2values, bytes2values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values is null");

        int[] offsets2offsetsLen = {0};
        byte[] bytes2offsetsLen = {};
        assertThatThrownBy(
            () -> new VariableWidthOmniBlock(1, 2, (VarcharVec) baseBlock.getValues(), offsets2offsetsLen,
                bytes2offsetsLen)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("offsets length is less than positionCount");

        byte[] bytes4 = {0};
        assertThatThrownBy(
            () -> new VariableWidthOmniBlock(1, 4, (VarcharVec) baseBlock.getValues(), null, bytes4)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("offsets is null");

        int[] offsets5 = new int[6];
        byte[] bytes5 = {0};
        assertThatThrownBy(() -> new VariableWidthOmniBlock(1, 4, (VarcharVec) baseBlock.getValues(), offsets5, bytes5))
            .isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("valueIsNull length is less than positionCount");

        StringBuilder buffer = new StringBuilder();
        Slice slice = Slices.wrappedBuffer(buffer.toString().getBytes());
        byte[] bytes2Array = {0};
        assertThatThrownBy(
            () -> new VariableWidthOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, -1, slice, offsets, bytes2Array))
            .isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(() -> new VariableWidthOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 0, -1, slice, offsets,
            bytes2Array)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("positionCount is negative");

        assertThatThrownBy(() -> new VariableWidthOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 0, 0, null, offsets,
            bytes2Array)).isInstanceOfAny(IllegalArgumentException.class).hasMessageMatching("slice is null");

        int[] offsets2valueIsNullLen = new int[6];
        assertThatThrownBy(
            () -> new VariableWidthOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, slice, offsets2valueIsNullLen,
                bytes2Array)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("valueIsNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        Block baseBlock = buildBlockByBuilder();
        Block VariableWidthOmniBlock = new VariableWidthOmniBlock(4, (VarcharVec) baseBlock.getValues());
        long expect = 10;
        long expectSizeBytes = 39;
        long expectStates = 5;
        boolean[] position = {true, true, true, true};
        assertEquals(VariableWidthOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(VariableWidthOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(VariableWidthOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(VariableWidthOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        VariableWidthOmniBlock.close();
    }

    private Block buildBlockByBuilder() {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(blockBuilder, "alice");
        VARCHAR.writeString(blockBuilder, "bob");
        VARCHAR.writeString(blockBuilder, "charlie");
        VARCHAR.writeString(blockBuilder, "dave");
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());
    }

    private VariableWidthOmniBlock getBlock(int count) {
        // returns test data
        int[] offsets = new int[count + 1];
        int offset = 0;
        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < count; i++) {
            offsets[i + 1] = offset;
            String value = "value" + i;
            buffer.append(value);
            offset += value.getBytes().length;
        }
        Slice slice = Slices.wrappedBuffer(buffer.toString().getBytes());
        return new VariableWidthOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, count, slice, offsets,
            Optional.empty());
    }

    private BloomFilter getBf(int size) {
        Random rnd = new Random();

        BloomFilter bf = new BloomFilter(size, 0.01);
        for (int i = 0; i < 100; i++) {
            bf.test(("value" + rnd.nextLong()).getBytes());
        }
        return bf;
    }

    private static void assertBlockEquals(Block actual, VarcharVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(new String((byte[]) actual.get(position)), new String(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
