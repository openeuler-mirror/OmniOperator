/*
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
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestVectorReference
{
    @Test
    public void testLongVecReference()
    {
        LongVec parent = new LongVec(4 * 10);
        for (int i = 0; i < 40; i++) {
            parent.set(i, i);
        }
        LongVec subVec1 = parent.slice(0, 10);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i, subVec1.get(i));
        }
        Assert.assertEquals(false, subVec1.close());

        LongVec subVec2 = parent.slice(10, 20);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 10, subVec2.get(i));
        }
        Assert.assertEquals(false, subVec2.close());

        LongVec subVec3 = parent.slice(20, 30);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 20, subVec3.get(i));
        }
        Assert.assertEquals(false, subVec1.close());

        LongVec subVec4 = parent.slice(30, 40);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 30, subVec4.get(i));
        }
        Assert.assertEquals(false, subVec4.close());

        Assert.assertEquals(true, parent.close());
    }

    @Test
    public void testIntVecReference()
    {
        IntVec parent = new IntVec(4 * 10);
        for (int i = 0; i < 40; i++) {
            parent.set(i, i);
        }
        IntVec subVec1 = parent.slice(0, 10);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i, subVec1.get(i));
        }
        Assert.assertEquals(false, subVec1.close());
        IntVec subVec2 = parent.slice(10, 20);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 10, subVec2.get(i));
        }
        Assert.assertEquals(false, subVec2.close());
        IntVec subVec3 = parent.slice(20, 30);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 20, subVec3.get(i));
        }
        Assert.assertEquals(false, subVec3.close());
        IntVec subVec4 = parent.slice(30, 40);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(i + 30, subVec4.get(i));
        }
        Assert.assertEquals(false, subVec4.close());

        Assert.assertEquals(true, parent.close());
    }

    @Test
    public void testDoubleVecReference()
    {
        DoubleVec parent = new DoubleVec(4 * 10);
        for (int i = 0; i < 40; i++) {
            parent.set(i, (double) i / 3);
        }
        DoubleVec subVec1 = parent.slice(0, 10);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((double) i / 3, subVec1.get(i));
        }
        Assert.assertEquals(false, subVec1.close());

        DoubleVec subVec2 = parent.slice(10, 20);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((double) (i + 10) / 3, subVec2.get(i));
        }
        Assert.assertEquals(false, subVec2.close());

        DoubleVec subVec3 = parent.slice(20, 30);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((double) (i + 20) / 3, subVec3.get(i));
        }
        Assert.assertEquals(false, subVec3.close());

        DoubleVec subVec4 = parent.slice(30, 40);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((double) (i + 30) / 3, subVec4.get(i));
        }
        Assert.assertEquals(false, subVec4.close());

        Assert.assertEquals(true, parent.close());
    }

    @Test
    public void testMultiVecRef()
    {
        DoubleVec parent = new DoubleVec(4 * 10);
        for (int i = 0; i < 40; i++) {
            parent.set(i, (double) i / 3);
        }
        //create sub slice
        DoubleVec slice1 = parent.slice(0, 20);
        //create parent before sub vec close.
        Assert.assertEquals(false, parent.close());
        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((double) i / 3, slice1.get(i));
        }

        //create slice1 sub slice
        DoubleVec childSlice1 = slice1.slice(0, 5);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals((double) i / 3, childSlice1.get(i));
        }
        Assert.assertEquals(false, childSlice1.close());
        Assert.assertEquals(true, slice1.close());
    }

    @Test(expectedExceptions = OmniRuntimeException.class)
    public void testSliceVecNotSupportWritable()
    {
        DoubleVec parent = new DoubleVec(4 * 10);
        for (int i = 0; i < 40; i++) {
            parent.set(i, (double) i / 3);
        }
        DoubleVec slice1 = parent.slice(0, 20);
        //slice vec not support writable
        slice1.set(0, (double) 1000 / 3);
    }
}
