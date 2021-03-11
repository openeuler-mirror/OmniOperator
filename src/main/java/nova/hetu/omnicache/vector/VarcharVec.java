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
package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class VarcharVec extends VariableWidthVec
{
    public VarcharVec(int capacity, int elements)
    {
        super(capacity, elements);
    }

    public VarcharVec(ByteBuffer buffer, int[] offsets, int[] lengths)
    {
        super(buffer);
        this.offsets = offsets;
        this.lengths = lengths;
        this.capacity = buffer.capacity();
    }

    @Override
    public VariableWidthVec slice(int startPosition, int endPosition)
    {
        int elementCount = endPosition - startPosition;
        int capacity = 0;
        int[] newOffsets = new int[elementCount];
        int[] newLengths = new int[elementCount];
        for (int i=0; i<elementCount; i++) {
            newOffsets[i] = offsets[i + startPosition] - offsets[startPosition];
            newLengths[i] = lengths[i + startPosition];
            capacity = capacity + lengths[i + startPosition];
        }
        VarcharVec newVec = new VarcharVec(capacity, elementCount);
        this.data.position(offsets[startPosition]);
        byte[] region = new byte[capacity];
        this.data.get(region, 0, capacity);
        newVec.setData(region);
        newVec.set(newOffsets, newLengths);
        return newVec;
    }


    public VariableWidthVec sliceByOffset(int startOff, int endOff)
    {
        int startIdx = Arrays.binarySearch(offsets, startOff); // has to start at the beginning of an element
        int curIdx = startIdx;
        int elementCount = 0;
        int currentPosition = startOff;
        int totalLength = endOff - startOff;
        while (currentPosition <= endOff) {
            currentPosition = currentPosition + lengths[curIdx];
            elementCount ++;
            curIdx ++;
        }
        int[] newOffsets = new int[elementCount];
        int[] lengths = new int[elementCount];
        for (int i=0; i< elementCount; i++) {
            newOffsets[i] = offsets[startIdx + i] - startOff;
            lengths[i] = lengths[startIdx + i];
        }
        VarcharVec newVec = new VarcharVec(totalLength, elementCount);
        this.data.position(startIdx);
        byte[] region = new byte[totalLength];
        this.data.get(region, 0, totalLength);
        newVec.setData(region);
        newVec.set(newOffsets, lengths);
        return newVec;
    }

    public int[] getOffsets()
    {
        return this.offsets;
    }

    public int[] getLengths()
    {
        return this.lengths;
    }

    public int getLength(int position)
    {
        int startIdx = Arrays.binarySearch(offsets, position);
        return lengths[startIdx];
    }

    public void set(int idx, int offset, int length)
    {
        this.offsets[idx] = offset;
        this.lengths[idx] = length;
    }

    public void set(int[] offsets, int[] lengths)
    {
        this.offsets = offsets;
        this.lengths = lengths;
    }

    public void setData(byte[] data)
    {
        this.data.put(data, 0, data.length);
    }

    public void setData(int position, byte[] data)
    {
        this.data.position(position);
        this.data.put(data, 0, data.length);
    }

    public byte[] getData(int idx)
    {
        if (lengths[idx] == 0) {
            return "".getBytes();
        } else {
            byte[] output = new byte[lengths[idx]];
            int length = lengths[idx];
            int offset = offsets[idx];
            this.data.position(offset);
            this.data.get(output, 0, length);
            return output;
        }
    }

    public byte[] getDataAtOffset(int position)
    {
        int idx = Arrays.binarySearch(offsets, position);
        byte[] output = new byte[lengths[idx]];
        int length = lengths[idx];
        int offset = offsets[idx];
        this.data.position(offset);
        this.data.get(output, 0, length);
        return output;
    }

    public byte[] getData(int idx, int length)
    {
        byte[] output = new byte[length];
        int offset = offsets[idx];
        this.data.position(offset);
        this.data.get(output, 0, length);
        return output;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(int other)
    {
        return null;
    }

    @Override
    public Vec mmul(Vec other)
    {
        return null;
    }

    @Override
    public Vec filter()
    {
        return null;
    }

    @Override
    public Vec groupby()
    {
        return null;
    }

    @Override
    public Vec concat(Vec other)
    {
        return null;
    }

    @Override
    public Vec join(Vec other)
    {
        return null;
    }

    @Override
    public VecType getType()
    {
        return VecType.DOUBLE;
    }

    @Override
    public ByteBuffer getData()
    {
        return null;
    }
}