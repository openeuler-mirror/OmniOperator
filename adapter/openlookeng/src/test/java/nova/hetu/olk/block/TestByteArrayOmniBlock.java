package nova.hetu.olk.block;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestByteArrayOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlockByBuilder();
        BooleanVec booleanVec = new BooleanVec(4);
        booleanVec.set(0, false);
        booleanVec.set(1, true);
        booleanVec.set(2, false);
        booleanVec.set(3, true);
        ByteArrayOmniBlock byteArrayOmniBlock = new ByteArrayOmniBlock(4, booleanVec);
        assertBlockEquals(BOOLEAN, byteArrayOmniBlock, baseBlock);
        assertEquals(baseBlock.toString(), byteArrayOmniBlock.toString());

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        byteArrayOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (size == booleanVec.getCapacityInBytes()) {
                isIdentical.set(true);
            }
        });
        assertTrue(isIdentical.get());

        Block byteArrayOmniBlockRegion = byteArrayOmniBlock.getRegion(2, 2);
        assertEquals(byteArrayOmniBlockRegion.getPositionCount(), 2);

        for (int i = 0; i < byteArrayOmniBlockRegion.getPositionCount(); i++) {
            assertEquals(byteArrayOmniBlockRegion.get(i), byteArrayOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (BooleanVec) baseBlock.getValues());

        baseBlock.close();
        byteArrayOmniBlock.close();
        byteArrayOmniBlockRegion.close();
        actualBlock.close();
    }

    @Test
    public void testInvalidInput() {
        byte[] bytes = {};
        byte[] values = {};
        assertThatThrownBy(
            () -> new ByteArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, 1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");
        assertThatThrownBy(
            () -> new ByteArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, -1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");
        assertThatThrownBy(
            () -> new ByteArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");
        byte[] values2len = new byte[6];
        assertThatThrownBy(() -> new ByteArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 0, 4, bytes,
            values2len)).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("isNull length is less than positionCount");

        Block baseBlock = buildBlockByBuilder();
        BooleanVec booleanVec = (BooleanVec) baseBlock.getValues();
        byte[] bytes2array = {};
        assertThatThrownBy(() -> new ByteArrayOmniBlock(-1, 4, bytes2array, booleanVec)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");
        assertThatThrownBy(() -> new ByteArrayOmniBlock(1, -1, bytes2array, booleanVec)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");
        assertThatThrownBy(() -> new ByteArrayOmniBlock(1, 6, bytes2array, booleanVec)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");
        assertThatThrownBy(() -> new ByteArrayOmniBlock(1, 4, bytes2array, booleanVec)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        BooleanVec booleanVec = new BooleanVec(4);
        booleanVec.set(0, false);
        booleanVec.set(1, true);
        booleanVec.set(2, false);
        booleanVec.set(3, true);
        Block byteArrayOmniBlock = new ByteArrayOmniBlock(4, booleanVec);
        long expect = 2L;
        long expectSizeBytes = 8L;
        long expectStates = 1L;
        boolean[] position = {true, true, true, true};
        assertEquals(byteArrayOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(byteArrayOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(byteArrayOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(byteArrayOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);
        byteArrayOmniBlock.close();
    }

    @Test
    public void testCopyRegion() {
        BooleanVec booleanVec = new BooleanVec(4);
        booleanVec.set(0, false);
        booleanVec.set(1, true);
        booleanVec.set(2, false);
        booleanVec.set(3, true);
        Block byteArrayOmniBlock = new ByteArrayOmniBlock(4, booleanVec);
        Block copyRegionBlock = byteArrayOmniBlock.copyRegion(0, byteArrayOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock, (BooleanVec) byteArrayOmniBlock.getValues());

        Block copyNotEqualRegionBlock = byteArrayOmniBlock.copyRegion(0, 3);
        assertBlockEquals(copyNotEqualRegionBlock, (BooleanVec) byteArrayOmniBlock.getValues());

        copyNotEqualRegionBlock.close();
    }

    @Test
    public void testCopyPosition() {
        BooleanVec booleanVec = new BooleanVec(4);
        booleanVec.set(0, false);
        booleanVec.set(1, true);
        booleanVec.set(2, false);
        booleanVec.set(3, true);
        Block byteArrayOmniBlock = new ByteArrayOmniBlock(4, booleanVec);

        int[] positions = {0, 2, 3};
        Block copyPositionsBlock = byteArrayOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyPositionsBlock.getByte(i, 0), byteArrayOmniBlock.getByte(positions[i], 0));
        }
        byteArrayOmniBlock.close();
        copyPositionsBlock.close();
    }

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 4);
        blockBuilder.appendNull();
        BOOLEAN.writeBoolean(blockBuilder, false);
        blockBuilder.appendNull();
        BOOLEAN.writeBoolean(blockBuilder, false);
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());

        ByteArrayOmniBlock nullByteArrayOmniBlock = new ByteArrayOmniBlock(4, (BooleanVec) block.getValues());
        // build block from vec
        AssertJUnit.assertTrue(nullByteArrayOmniBlock.isNull(0));
        AssertJUnit.assertTrue(nullByteArrayOmniBlock.isNull(2));

        nullByteArrayOmniBlock.close();
    }

    @Test
    public void testFilter() {
        int count = 4;
        int size = 4;
        boolean[] valid = new boolean[count];
        Arrays.fill(valid, Boolean.TRUE);
        ByteArrayOmniBlock block = getBlock(count);
        Byte[] values = new Byte[block.getPositionCount()];

        BloomFilter bf = getBf(size);
        for (int i = 0; i < block.getPositionCount(); i++) {
            values[i] = (block.getByte(i, 0));
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

    private Block buildBlockByBuilder() {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 4);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, true);
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());
    }

    private ByteArrayOmniBlock getBlock(int count) {
        BooleanVec booleanVec = new BooleanVec(count);
        for (int i = 0; i < count; i++) {
            if ((i & 1) == 1) {
                booleanVec.set(i, true);
            } else {
                booleanVec.set(i, false);
            }
        }
        return new ByteArrayOmniBlock(count, booleanVec);
    }

    private BloomFilter getBf(int size) {
        Random rnd = new Random();
        BloomFilter bf = new BloomFilter(size, 0.01);
        for (int i = 0; i < 100; i++) {
            bf.test(("value" + rnd.nextLong()).getBytes());
        }
        return bf;
    }

    private static void assertBlockEquals(Block actual, BooleanVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals((actual.get(position)), (expected.get(position)) ? (byte) 1 : (byte) 0);
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
