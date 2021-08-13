/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.LongVec;

import org.testng.annotations.Test;

public class LongArrayOmniBlockTest {
    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, Long.MAX_VALUE);
        Block onHeapBlock = blockBuilder.build();
        Block block = OperatorUtils.getOffHeapBlock(onHeapBlock);

        assertTrue(block.isNull(0));
        assertEquals(BIGINT.getLong(block, 1), 42L);
        assertTrue(block.isNull(2));
        assertEquals(BIGINT.getLong(block, 3), Long.MAX_VALUE);

        // build block from vec
        Block block1 = new LongArrayOmniBlock(4, (LongVec) block.getValues());
        assertTrue(block1.isNull(0));
        assertEquals(BIGINT.getLong(block1, 1), 42L);
        assertTrue(block1.isNull(2));
        assertEquals(BIGINT.getLong(block1, 3), Long.MAX_VALUE);
        block.close();
    }
}
