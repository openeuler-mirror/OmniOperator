package nova.hetu.olk.block;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.DictionaryId;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;

import java.util.Arrays;
import java.util.Random;

import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.annotations.Test;
import java.util.concurrent.atomic.AtomicBoolean;


import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TestDictionaryOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testBasicFunc() {
        // build vec through vec
        int[] ids = {0, 1, 2, 3};
        Block baseBlock = buildBlockByBuilder();
        DictionaryOmniBlock dictionaryOmniBlock = new DictionaryOmniBlock((Vec) baseBlock.getValues(), ids);
        assertBlockEquals(VARCHAR, dictionaryOmniBlock, baseBlock);

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        dictionaryOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (part == ids) {
                isIdentical.set(true);
            }
        });
        assertTrue(isIdentical.get());

        DictionaryId dictionaryId = dictionaryOmniBlock.getDictionarySourceId();
        int[] positions = new int[11];
        DictionaryOmniBlock interceptBlock = (DictionaryOmniBlock) dictionaryOmniBlock.getPositions(positions, 0, 4);
        assertEquals(interceptBlock.getDictionarySourceId(), dictionaryId);

        Block regionDicOmniBlock = dictionaryOmniBlock.getRegion(2, 2);
        assertEquals(regionDicOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionDicOmniBlock.getPositionCount(); i++) {
            assertEquals(regionDicOmniBlock.get(i), dictionaryOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, dictionaryOmniBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (VarcharVec) baseBlock.getValues());

        baseBlock.close();
        dictionaryOmniBlock.close();
        regionDicOmniBlock.close();
        interceptBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlockByBuilder();
        int[] ids = {0, 1, 2, 3};
        Block dictionaryOmniBlock = new DictionaryOmniBlock((Vec) baseBlock.getValues(), ids);
        Block copyRegionBlock = dictionaryOmniBlock.copyRegion(0, dictionaryOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock,
            (VarcharVec) ((DictionaryOmniBlock) dictionaryOmniBlock).getDictionary().getValues());

        Block compactDicBlock = buildBlock2Compact();
        int[] ids1 = {0, 1, 2, 3};
        Block newBlock2Compact = new DictionaryOmniBlock((Vec) compactDicBlock.getValues(), ids1);
        Block copyRegionBlock2Compact = newBlock2Compact.copyRegion(0, newBlock2Compact.getPositionCount());
        assertBlockEquals(copyRegionBlock2Compact,
            (VarcharVec) ((DictionaryOmniBlock) newBlock2Compact).getDictionary().getValues());

        baseBlock.close();
        dictionaryOmniBlock.close();
        copyRegionBlock.close();
        compactDicBlock.close();
        newBlock2Compact.close();
        copyRegionBlock2Compact.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlockByBuilder();
        int[] ids = {0, 1, 2, 3};
        Block dictionaryOmniBlock = new DictionaryOmniBlock((Vec) baseBlock.getValues(), ids);
        int[] positions = {0, 2, 3};
        Block copyRegionBlock = dictionaryOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyRegionBlock.getString(i, 0, 0), dictionaryOmniBlock.getString(positions[i], 0, 0));
        }

        baseBlock.close();
        dictionaryOmniBlock.close();
        copyRegionBlock.close();
    }

    @Test
    public void testFilter() {
        int count = 4;
        int size = 1000;
        boolean[] valid = new boolean[count];
        Arrays.fill(valid, Boolean.TRUE);
        DictionaryOmniBlock block = getBlock(count);
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
    public void testGet() {
        Block baseBlock = buildBlockByBuilder();
        int[] ids = {0, 1, 2, 3};
        DictionaryOmniBlock dictionaryOmniBlock = new DictionaryOmniBlock((Vec) baseBlock.getValues(), ids);
        long expect = 14L;
        long expectSizeBytes = 55L;
        long expectStates = 5L;
        boolean[] position = {true, true, true, true};
        long expect2LogicalSizeInBytes = 39L;
        String expectStr = "DictionaryOmniBlock{positionCount=4}";
        assertEquals(dictionaryOmniBlock.toString(), expectStr);
        assertEquals(dictionaryOmniBlock.getLogicalSizeInBytes(), expect2LogicalSizeInBytes);
        assertEquals(dictionaryOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(dictionaryOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(dictionaryOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(dictionaryOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        Block loadedOmniBlock = dictionaryOmniBlock.getLoadedBlock();
        assertBlockEquals(VARCHAR, dictionaryOmniBlock, loadedOmniBlock);

        baseBlock.close();
        loadedOmniBlock.close();
    }

    @Test
    public void testInvalidInput() {
        Block baseBlock = buildBlockByBuilder();
        int[] ids = {0, 1, 2, 3};
        DictionaryOmniBlock throwDicOmniBlock = new DictionaryOmniBlock((Vec) baseBlock.getValues(), ids);
        assertThatThrownBy(() -> new DictionaryOmniBlock(1, -1, (Vec) baseBlock.getValues(), ids, false,
            throwDicOmniBlock.getDictionarySourceId())).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("positionCount is negative");
        assertThatThrownBy(() -> new DictionaryOmniBlock(1, 4, (Vec) baseBlock.getValues(), ids, false,
            throwDicOmniBlock.getDictionarySourceId())).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("ids length is less than positionCount");

        assertThatThrownBy(
            () -> new DictionaryOmniBlock(1, -1, (DictionaryVec) throwDicOmniBlock.getValues(), ids, baseBlock, false,
                throwDicOmniBlock.getDictionarySourceId())).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("positionCount is negative");

        assertThatThrownBy(
            () -> new DictionaryOmniBlock(1, 4, (DictionaryVec) throwDicOmniBlock.getValues(), ids, baseBlock, false,
                throwDicOmniBlock.getDictionarySourceId())).isInstanceOfAny(IllegalArgumentException.class)
            .hasMessageMatching("ids length is less than positionCount");

        baseBlock.close();
        throwDicOmniBlock.close();
    }

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "alice");
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "bob");
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());

        int[] ids = {0, 1, 2, 3};
        DictionaryOmniBlock nullDicOmniBlock = new DictionaryOmniBlock((Vec) block.getValues(), ids);
        // build block from vec
        assertTrue(nullDicOmniBlock.isNull(0));
        assertTrue(nullDicOmniBlock.isNull(2));

        nullDicOmniBlock.close();
        block.close();
    }

    private Block buildBlockByBuilder() {
        BlockBuilder dictionaryOmniBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(dictionaryOmniBuilder, "alice");
        VARCHAR.writeString(dictionaryOmniBuilder, "bob");
        VARCHAR.writeString(dictionaryOmniBuilder, "charlie");
        VARCHAR.writeString(dictionaryOmniBuilder, "dave");
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, dictionaryOmniBuilder.build());
    }

    private Block buildBlock2Compact() {
        BlockBuilder dictionaryOmniBuilder = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeString(dictionaryOmniBuilder, "alice");
        VARCHAR.writeString(dictionaryOmniBuilder, "bob");
        VARCHAR.writeString(dictionaryOmniBuilder, "charlie");
        VARCHAR.writeString(dictionaryOmniBuilder, "dave");
        VARCHAR.writeString(dictionaryOmniBuilder, "dave");
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, dictionaryOmniBuilder.build());
    }

    private DictionaryOmniBlock getBlock(int count) {
        Block baseBlock = buildBlockByBuilder();
        int[] ids = {0, 1, 2, 3};
        Vec dictionary = (Vec) baseBlock.getValues();
        DictionaryOmniBlock dictionaryOmniBlock = new DictionaryOmniBlock(count, dictionary, ids);
        baseBlock.close();
        return dictionaryOmniBlock;
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
