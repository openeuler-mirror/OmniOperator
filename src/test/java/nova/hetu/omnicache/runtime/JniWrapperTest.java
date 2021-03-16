/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

public class JniWrapperTest
{

    JniWrapper wrapper;

    @BeforeClass
    public void setUp() {
        wrapper = new JniWrapper();
    }

    @Test
    public void testOrderByOneColumn()
    {
        int[] sourceTypes = {0};
        int[] outputCols = {0};
        int[] sortCols = {0};
        int[] ascendings = {1};
        int[] nullFirsts = {0};

        long sortAddress = wrapper.allocAndInitSort(sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        ByteBuffer[] inputs = new ByteBuffer[1];
        IntVec vec = new IntVec(8);
        vec.set(0, 5);
        vec.set(1, 3);
        vec.set(2, 2);
        vec.set(3, 6);
        vec.set(4, 1);
        vec.set(5, 4);
        vec.set(6, 7);
        vec.set(7, 8);
        inputs[0] = vec.getData();
        int[][] nulls = {{0, 0, 0, 0, 0, 0, 0, 0}};

        wrapper.addTable(sortAddress, inputs, nulls, 1, 8);
        long addressesAddr = wrapper.sort(sortAddress);
        OMResult result = wrapper.getResult(sortAddress, addressesAddr);

        ByteBuffer[] output = result.getBuffers();
        int len = result.getLength();
        int[] actual = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual[i] = output[0].getInt(i * Integer.BYTES);
        }
        int[] expectd = {1, 2, 3, 4, 5, 6, 7, 8};
        Assert.assertEquals(actual, expectd);
    }
}
