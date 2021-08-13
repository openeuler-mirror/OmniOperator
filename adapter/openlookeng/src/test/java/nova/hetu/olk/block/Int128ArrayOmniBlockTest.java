/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import io.prestosql.spi.block.Block;
import nova.hetu.omniruntime.vector.Decimal128Vec;

import org.testng.annotations.Test;

import java.util.Optional;

public class Int128ArrayOmniBlockTest
{

   @Test
   public void testMultipleValuesWithNull()
   {
       int positionCount = 4;
       long[] values = {0L, 0L, 0L, 42L, 0L, 0L, Long.MAX_VALUE, Long.MAX_VALUE};
       boolean[] valueIsNull = {true, false, true, false};
       Int128ArrayOmniBlock block = new Int128ArrayOmniBlock(positionCount, Optional.of(valueIsNull), values);

       assertTrue(block.isNull(0));
       assertEquals(block.get(1), new long[]{0L, 42L});
       assertTrue(block.isNull(2));
       assertEquals(block.get(3), new long[]{Long.MAX_VALUE, Long.MAX_VALUE});

       // build block from vec
       Block block1 = new Int128ArrayOmniBlock(4, (Decimal128Vec) block.getValues());
       assertTrue(block1.isNull(0));
       assertEquals(block1.get(1), new long[]{0L, 42L});
       assertTrue(block1.isNull(2));
       assertEquals(block.get(3), new long[]{Long.MAX_VALUE, Long.MAX_VALUE});
       block.close();
   }
}
