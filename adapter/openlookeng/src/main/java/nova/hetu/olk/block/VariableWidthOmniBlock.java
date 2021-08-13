/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.compactArray;
import static io.prestosql.spi.block.BlockUtil.compactOffsets;
import static nova.hetu.olk.tool.BlockUtils.compactVec;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.AbstractVariableWidthBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.omniruntime.vector.VarcharVec;

import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nullable;

/**
 * The type Variable width omni block.
 *
 * @since 20210630
 */
public class VariableWidthOmniBlock extends AbstractVariableWidthBlock<byte[]> {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthOmniBlock.class).instanceSize();

    private final int arrayOffset;

    private final int positionCount;

    private final VarcharVec values;

    private final int[] offsets;

    @Nullable
    private final boolean[] valueIsNull;

    private final long retainedSizeInBytes;

    private final long sizeInBytes;

    /**
     * Instantiates a new Variable width omni block.
     *
     * @param positionCount the position count
     * @param slice the slice
     * @param offsets the offsets
     * @param valueIsNull the value is null
     */
    public VariableWidthOmniBlock(int positionCount, Slice slice, int[] offsets, Optional<boolean[]> valueIsNull) {
        this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
    }

    /**
     * Instantiates a new Variable width omni block.
     *
     * @param arrayOffset the array offset
     * @param positionCount the position count
     * @param slice the slice
     * @param offsets the offsets
     * @param valueIsNull the value is null
     */
    VariableWidthOmniBlock(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull) {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }

        int dataLength = offsets[arrayOffset + positionCount] - offsets[arrayOffset];
        this.values = new VarcharVec(dataLength, positionCount);

        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = compactOffsets(offsets, arrayOffset, positionCount);
        if (slice.hasByteArray()) {
            this.values.put(0, slice.byteArray(), slice.byteArrayOffset(), this.offsets, 0, positionCount);
        }

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }

        if (valueIsNull != null) {
            this.values.setNulls(0, valueIsNull, arrayOffset, positionCount);
            this.valueIsNull = compactArray(valueIsNull, arrayOffset, positionCount);
        } else {
            this.valueIsNull = null;
        }

        this.arrayOffset = 0;

        sizeInBytes = offsets[this.arrayOffset + positionCount] - offsets[this.arrayOffset] + (
            (Integer.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);
    }

    /**
     * this method for the native operator transform vec to block
     *
     * @param positionCount positionCount
     * @param values the values int vector
     */
    public VariableWidthOmniBlock(int positionCount, VarcharVec values) {
        this(positionCount, values, values.getRawValueOffset(),
            values.hasNullValue() ? Optional.of(values.getRawValueNulls()) : Optional.empty());
    }

    /**
     * Instantiates a new Variable width omni block.
     *
     * @param positionCount the position count
     * @param values the values
     * @param offsets the offsets
     * @param valuesIsNull the values is null
     */
    public VariableWidthOmniBlock(int positionCount, VarcharVec values, int[] offsets,
        Optional<boolean[]> valuesIsNull) {
        this(values.getOffset(), positionCount, values, offsets, valuesIsNull.orElse(null));
    }

    /**
     * Instantiates a new Variable width omni block.
     *
     * @param arrayOffset the array offset
     * @param positionCount the position count
     * @param values the values
     * @param offsets the offsets
     * @param valueIsNull the value is null
     */
    public VariableWidthOmniBlock(int arrayOffset, int positionCount, VarcharVec values, int[] offsets,
        boolean[] valueIsNull) {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values == null) {
            throw new IllegalArgumentException("slice is null");
        }

        this.values = values;

        if (offsets != null && offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        if (offsets == null) {
            throw new IllegalArgumentException("offsets is null");
        }

        this.offsets = offsets;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }

        this.valueIsNull = valueIsNull;
        this.arrayOffset = arrayOffset;

        sizeInBytes = offsets[arrayOffset + positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES)
            * (long) positionCount);
        retainedSizeInBytes = INSTANCE_SIZE + values.getCapacityInBytes() + sizeOf(valueIsNull) + sizeOf(offsets);
    }

    @Override
    protected final int getPositionOffset(int position) {
        return offsets[position + arrayOffset];
    }

    @Override
    public int getSliceLength(int position) {
        checkReadablePosition(position);
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    public boolean mayHaveNull() {
        return valueIsNull != null;
    }

    @Override
    protected boolean isEntryNull(int position) {
        return valueIsNull != null && valueIsNull[position + arrayOffset];
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length) {
        return offsets[arrayOffset + position + length] - offsets[arrayOffset + position] + (
            (Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions) {
        long sizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                sizeInBytes += offsets[arrayOffset + i + 1] - offsets[arrayOffset + i];
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer) {
        consumer.accept(values, (long) values.getCapacityInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length) {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (newValueIsNull != null && isEntryNull(position)) {
                newValueIsNull[i] = true;
            }
            newOffsets[i + 1] = newOffsets[i] + getSliceLength(position);
        }
        VarcharVec newValues = values.copyPositions(positions, offset, length);
        return new VariableWidthOmniBlock(0, length, newValues, newOffsets, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position) {
        // use slice wrapped byteBuffer for zero-copy data
        values.getValues().position(0);
        if (values.getValues().capacity() != 0) {
            return Slices.wrappedBuffer(values.getValues());
        }

        // empty values
        return Slices.wrappedBuffer();
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        checkValidRegion(getPositionCount(), positionOffset, length);
        VarcharVec newValues = values.slice(positionOffset, positionOffset + length);
        return new VariableWidthOmniBlock(newValues.getOffset(), length, newValues, offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length) {
        checkValidRegion(getPositionCount(), positionOffset, length);

        int[] newOffsets = compactOffsets(offsets, positionOffset + arrayOffset, length);
        VarcharVec newValues = compactVec(values, positionOffset, length);
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);

        if (newOffsets == offsets && newValues == values && newValueIsNull == valueIsNull) {
            return this;
        }

        return new VariableWidthOmniBlock(0, length, newValues, offsets, valueIsNull);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VariableWidthOmniBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", values=").append(values);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean[] filter(BloomFilter filter, boolean[] validPositions) {
        for (int i = 0; i < positionCount; i++) {
            byte[] value = values.getData(offsets[i + arrayOffset],
                offsets[i + arrayOffset + 1] - offsets[i + arrayOffset]);
            validPositions[i] = validPositions[i] && filter.test(value);
        }
        return validPositions;
    }

    @Override
    public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test) {
        int matchCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull != null && valueIsNull[positions[i] + arrayOffset]) {
                if (test.apply(null)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            } else {
                byte[] value = values.getData(offsets[i + arrayOffset],
                    offsets[i + arrayOffset + 1] - offsets[i + arrayOffset]);
                if (test.apply(value)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
        }

        return matchCount;
    }

    @Override
    public byte[] get(int position) {
        if (valueIsNull != null && valueIsNull[position + arrayOffset]) {
            return null;
        }
        return values.getData(offsets[position + arrayOffset],
            offsets[position + arrayOffset + 1] - offsets[position + arrayOffset]);
    }

    @Override
    public Object getValues() {
        return values;
    }

    @Override
    public void setClosable(boolean isClosable) {
        values.setClosable(isClosable);
    }

    @Override
    public boolean isExtensionBlock() {
        return true;
    }

    @Override
    public void close() {
        values.close();
    }
}
