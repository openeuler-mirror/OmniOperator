/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.annotations.Test;

public class TestDoubleArrayOmniBlock
{
    @Test(enabled = false)
    public void testMultipleValuesWithNull()
    {
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
}
