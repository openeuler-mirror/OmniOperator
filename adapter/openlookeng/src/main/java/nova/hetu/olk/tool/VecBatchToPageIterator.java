/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;

import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import java.util.Iterator;

/**
 * The type Vec batch to page iterator.
 *
 * @since 20210630
 */
public class VecBatchToPageIterator implements Iterator<Page> {
    private final Iterator<VecBatch> vecBatchIterator;

    /**
     * Instantiates a new Vec batch to page iterator.
     *
     * @param vecBatchIterator the vec batch iterator
     */
    public VecBatchToPageIterator(Iterator<VecBatch> vecBatchIterator) {
        this.vecBatchIterator = vecBatchIterator;
    }

    @Override
    public boolean hasNext() {
        return vecBatchIterator.hasNext();
    }

    @Override
    public Page next() {
        VecBatch vecBatch = vecBatchIterator.next();
        int positionCount = vecBatch.getRowCount();
        Vec[] vectors = vecBatch.getVectors();
        int channelCount = vectors.length;
        Block[] blocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            if (vectors[i] instanceof DoubleVec) {
                blocks[i] = new DoubleArrayOmniBlock(positionCount, ((DoubleVec) vectors[i]));
            } else if (vectors[i] instanceof BooleanVec) {
                blocks[i] = new ByteArrayOmniBlock(positionCount, ((BooleanVec) vectors[i]));
            } else if (vectors[i] instanceof LongVec) {
                blocks[i] = new LongArrayOmniBlock(positionCount, (LongVec) vectors[i]);
            } else if (vectors[i] instanceof IntVec) {
                blocks[i] = new IntArrayOmniBlock(positionCount, (IntVec) vectors[i]);
            } else if (vectors[i] instanceof VarcharVec) {
                blocks[i] = new VariableWidthOmniBlock(positionCount, (VarcharVec) vectors[i]);
            } else if (vectors[i] instanceof Decimal128Vec) {
                blocks[i] = new Int128ArrayOmniBlock(positionCount, (Decimal128Vec) vectors[i]);
            } else if (vectors[i] instanceof ContainerVec) {
                ContainerVec containerVec = (ContainerVec) vectors[i];
                blocks[i] = buildRowOmniBlock(containerVec);
            } else if (vectors[i] instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vectors[i];
                blocks[i] = new DictionaryOmniBlock(dictionaryVec, false, randomDictionaryId());
            } else {
                vecBatch.releaseAllVectors();
                vecBatch.close();
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported vector type " + vectors[i]);
            }
        }
        vecBatch.close();
        return new Page(positionCount, blocks);
    }

    private RowOmniBlock buildRowOmniBlock(ContainerVec containerVec) {
        VecType[] vecTypes = containerVec.getVecTypes();
        int positionCount = containerVec.getPositionCount();
        Block[] rowBlocks = new Block[vecTypes.length];
        int vectorCount = containerVec.getVecTypes().length;
        for (int vecIdx = 0; vecIdx < vectorCount; ++vecIdx) {
            VecType vecType = vecTypes[vecIdx];
            Block block;
            switch (vecType.getId()) {
                case OMNI_VEC_TYPE_INT :
                case OMNI_VEC_TYPE_DATE32 :
                    block = new IntArrayOmniBlock(positionCount, new IntVec(containerVec.getVector(vecIdx)));
                    rowBlocks[vecIdx] = block;
                    break;
                case OMNI_VEC_TYPE_LONG :
                case OMNI_VEC_TYPE_DECIMAL64 :
                    block = new LongArrayOmniBlock(positionCount, new LongVec(containerVec.getVector(vecIdx)));
                    rowBlocks[vecIdx] = block;
                    break;
                case OMNI_VEC_TYPE_DOUBLE :
                    block = new DoubleArrayOmniBlock(positionCount, new DoubleVec(containerVec.getVector(vecIdx)));
                    rowBlocks[vecIdx] = block;
                    break;
                case OMNI_VEC_TYPE_VARCHAR :
                case OMNI_VEC_TYPE_CHAR :
                    block = new VariableWidthOmniBlock(positionCount, new VarcharVec(containerVec.getVector(vecIdx)));
                    rowBlocks[vecIdx] = block;
                    break;
                case OMNI_VEC_TYPE_DECIMAL128 :
                    block = new Int128ArrayOmniBlock(positionCount,
                            new Decimal128Vec(containerVec.getVector(vecIdx), vecType));
                    rowBlocks[vecIdx] = block;
                    break;
                default :
                    throw new PrestoException(GENERIC_INTERNAL_ERROR,
                            "Unsupported vector type " + vecTypes[vecIdx].getId());
            }
        }
        int[] fieldBlockOffsets = new int[positionCount + 1];
        byte[] nulls = containerVec.getRawValueNulls();
        for (int position = 0; position < positionCount; position++) {
            fieldBlockOffsets[position + 1] = fieldBlockOffsets[position] + nulls[position] == Vec.NULL ? 0 : 1;
        }
        return new RowOmniBlock(0, positionCount, nulls, fieldBlockOffsets, rowBlocks);
    }
}
