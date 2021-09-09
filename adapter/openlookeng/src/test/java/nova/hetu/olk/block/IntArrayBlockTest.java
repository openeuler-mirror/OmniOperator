/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.IntVec;

import org.testng.annotations.Test;

public class IntArrayBlockTest
{
   @Test
   public void testMultipleValuesWithNull()
   {
       BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, 10);
       blockBuilder.appendNull();
       INTEGER.writeLong(blockBuilder, 42);
       blockBuilder.appendNull();
       INTEGER.writeLong(blockBuilder, Integer.MAX_VALUE);
       Block onHeapBlock = blockBuilder.build();
       Block block = OperatorUtils.getOffHeapBlock(onHeapBlock);

       assertTrue(block.isNull(0));
       assertEquals(INTEGER.getLong(block, 1), 42);
       assertTrue(block.isNull(2));
       assertEquals(INTEGER.getLong(block, 3), Integer.MAX_VALUE);

       // build block from vec
       Block block1 = new IntArrayOmniBlock(4, (IntVec) block.getValues());
       assertTrue(block1.isNull(0));
       assertEquals(INTEGER.getLong(block1, 1), 42);
       assertTrue(block1.isNull(2));
       assertEquals(INTEGER.getLong(block1, 3), Integer.MAX_VALUE);
       block.close();
   }

}
