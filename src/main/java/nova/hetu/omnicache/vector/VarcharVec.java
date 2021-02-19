package nova.hetu.omnicache.vector;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class VarcharVec extends VarVec
{
    private static final int BUFFER_MAX_SIZE = 300*1024;
    public VarcharVec(int capacity, int elements)
    {
        super(capacity, elements);
    }

    public VarcharVec(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public VarVec slice(int startPosition, int endPosition)
    {
        int startIdx = Arrays.binarySearch(offsets, startPosition);
        int elementCount = 0;
        int currentPosition = startPosition;
        int totalLength = endPosition - startPosition;
        while (currentPosition <= endPosition) {
            currentPosition = currentPosition + lengths[startIdx + 1];
            elementCount ++;
            startIdx ++;
        }
        int[] newOffsets = new int[elementCount];
        int[] lengths = new int[elementCount];
        startIdx = Arrays.binarySearch(offsets, startPosition);
        for (int i=0; i< elementCount; i++) {
            newOffsets[i] = offsets[startIdx + i] - startPosition;
            lengths[i] = lengths[startIdx + i];
        }
        VarcharVec newVec = new VarcharVec((endPosition - startPosition), elementCount);
        this.buffer.position(startIdx);
        byte[] region = new byte[totalLength];
        this.buffer.get(region, 0, totalLength);
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
        this.buffer.put(data, 0, data.length);
    }

    public void setData(int position, byte[] data)
    {
        this.buffer.position(position);
        this.buffer.put(data, 0, data.length);
    }

    public byte[] getData(int idx)
    {
        if (lengths[idx] == 0) {
            return "".getBytes();
        } else {
            byte[] output = new byte[lengths[idx]];
            int length = lengths[idx];
            int offset = offsets[idx];
            this.buffer.position(offset);
            this.buffer.get(output, 0, length);
            return output;
        }
    }

    public byte[] getDataAtOffset(int position)
    {
        int idx = Arrays.binarySearch(offsets, position);
        byte[] output = new byte[lengths[idx]];
        int length = lengths[idx];
        int offset = offsets[idx];
        this.buffer.position(offset);
        this.buffer.get(output, 0, length);
        return output;
    }

    public byte[] getData(int idx, int length)
    {
        byte[] output = new byte[length];
        int offset = offsets[idx];
        this.buffer.position(offset);
        this.buffer.get(output, 0, length);
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