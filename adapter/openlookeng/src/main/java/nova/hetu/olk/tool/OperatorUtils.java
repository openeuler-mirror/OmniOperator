/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;

import com.google.common.primitives.Ints;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.type.BooleanVecType;
import nova.hetu.omniruntime.type.ContainerVecType;
import nova.hetu.omniruntime.type.Date32VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The type Operator utils.
 *
 * @since 20210630
 */
public final class OperatorUtils {
    private static final Logger log = Logger.get(OperatorUtils.class);

    private OperatorUtils() {
    }

    /**
     * To vec types vec type [ ].
     *
     * @param types the types
     * @return the vec type [ ]
     */
    public static VecType[] toVecTypes(List<? extends Type> types) {
        VecType[] vecTypes = types.stream().map(OperatorUtils::toVecType).toArray(VecType[]::new);
        return vecTypes;
    }

    /**
     * To vec type vec type.
     *
     * @param type the type
     * @return the vec type
     */
    public static VecType toVecType(Type type) {
        return toVecType(type.getTypeSignature());
    }

    /**
     * To vec type vec type.
     *
     * @param signature the signature
     * @return the vec type
     */
    public static VecType toVecType(TypeSignature signature) {
        String base = signature.getBase();
        switch (base) {
            case StandardTypes.INTEGER:
                return IntVecType.INTEGER;
            case StandardTypes.BIGINT:
                return LongVecType.LONG;
            case StandardTypes.DOUBLE:
                return DoubleVecType.DOUBLE;
            case StandardTypes.BOOLEAN:
                return BooleanVecType.BOOLEAN;
            case StandardTypes.VARBINARY:
                // FIXME: the max varbinary length is 8000. when varchar support dynamic allocate, pls fix it.
                return new VarcharVecType(8000);
            case StandardTypes.VARCHAR:
            case StandardTypes.CHAR:
                int width = signature.getParameters().get(0).getLongLiteral().intValue();
                return new VarcharVecType(width);
            case StandardTypes.DECIMAL:
                int precision = signature.getParameters().get(0).getLongLiteral().intValue();
                int scale = signature.getParameters().get(1).getLongLiteral().intValue();
                if (precision <= MAX_SHORT_PRECISION) {
                    return new Decimal64VecType(precision, scale);
                }
                return new Decimal128VecType(precision, scale);
            case StandardTypes.DATE:
                return Date32VecType.DATE32;
            case StandardTypes.ROW:
                return ContainerVecType.CONTAINER;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + base);
        }
    }

    /**
     * Create expressions string [ ].
     *
     * @param columns the columns
     * @return the string [ ]
     */
    public static String[] createExpressions(List<Integer> columns) {
        return createExpressions(Ints.toArray(columns));
    }

    /**
     * Create expressions string [ ].
     *
     * @param columns the columns
     * @return the string [ ]
     */
    public static String[] createExpressions(int[] columns) {
        String[] expressions = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            expressions[i] = "#" + columns[i];
        }
        return expressions;
    }

    /**
     * Create blank vectors for given size and types.
     *
     * @param vecAllocator VecAllocator used to create vectors
     * @param vecTypes Vec types
     * @param totalPositions Size for all the vectors
     * @return List contains blank vectors
     */
    public static List<Vec> createBlankVectors(VecAllocator vecAllocator, VecType[] vecTypes, int totalPositions) {
        List<Vec> vecsResult = new ArrayList<>();
        for (int i = 0; i < vecTypes.length; i++) {
            VecType type = vecTypes[i];
            switch (type.getId()) {
                case OMNI_VEC_TYPE_INT:
                case OMNI_VEC_TYPE_DATE32:
                    vecsResult.add(new IntVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VEC_TYPE_LONG:
                case OMNI_VEC_TYPE_DECIMAL64:
                    vecsResult.add(new LongVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    vecsResult.add(new DoubleVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    vecsResult.add(new BooleanVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    // FIXME: now the width is hardcode to 100, we should support to get the width from type
                    vecsResult.add(new VarcharVec(vecAllocator, totalPositions * 100, totalPositions));
                    break;
                case OMNI_VEC_TYPE_DECIMAL128:
                    vecsResult.add(new Decimal128Vec(vecAllocator, totalPositions));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + type);
            }
        }
        return vecsResult;
    }

    /**
     * Transfer to off heap pages list.
     *
     * @param vecAllocator vector allocator
     * @param pages the pages
     * @return the list
     */
    public static List<Page> transferToOffHeapPages(VecAllocator vecAllocator, List<Page> pages) {
        List<Page> offHeapInput = new ArrayList<>();
        for (Page page : pages) {
            Block[] blocks = getOffHeapBlocks(vecAllocator, page.getBlocks());
            offHeapInput.add(new Page(blocks));
        }
        return offHeapInput;
    }

    /**
     * Transfer to off heap pages page.
     *
     * @param vecAllocator vector allocator
     * @param page the page
     * @return the page
     */
    public static Page transferToOffHeapPages(VecAllocator vecAllocator, Page page) {
        Block[] blocks = getOffHeapBlocks(vecAllocator, page.getBlocks());
        return new Page(blocks);
    }

    private static Block[] getOffHeapBlocks(VecAllocator vecAllocator, Block[] blocks) {
        Block[] res = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            res[i] = buildOffHeapBlock(vecAllocator, blocks[i]);
        }
        return res;
    }

    /**
     * Gets off heap block.
     *
     * @param vecAllocator vector allocator
     * @param block the block
     * @return the off heap block
     */
    public static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block) {
        return buildOffHeapBlock(vecAllocator, block, block.getClass().getSimpleName(), block.getPositionCount());
    }

    /**
     * Gets off heap block.
     *
     * @param vecAllocator vector allocator
     * @param block the block
     * @param type the actual block type, e.g. RunLengthEncodedBlock or DictionaryBlock
     * @param positionCount the position count of the block
     * @return the off heap block
     */
    public static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block, String type, int positionCount) {
        if (block.isExtensionBlock()) {
            return block;
        }
        boolean[] valueIsNull = new boolean[positionCount];
        switch (type) {
            case "IntArrayBlock": {
                int[] ints = new int[positionCount];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        ints[j] = (int) block.get(j);
                    }
                }
                return new IntArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), ints);
            }
            case "LongArrayBlock": {
                long[] longs = new long[positionCount];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        longs[j] = (long) block.get(j);
                    }
                }
                return new LongArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), longs);
            }
            case "DoubleArrayBlock": {
                double[] doubles = new double[positionCount];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        doubles[j] = (double) block.get(j);
                    }
                }
                return new DoubleArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), doubles);
            }
            case "Int128ArrayBlock": {
                long[] longs = new long[positionCount * 2];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        long[] data = (long[]) block.get(j);
                        longs[j * 2] = data[0];
                        longs[j * 2 + 1] = data[1];
                    }
                }
                return new Int128ArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), longs);
            }
            case "VariableWidthBlock": {
                return getVariableWidthOmniBlock(vecAllocator, block, positionCount, valueIsNull);
            }
            case "DictionaryBlock": {
                Block dicBlock = buildOffHeapBlock(vecAllocator, ((DictionaryBlock) block).getDictionary());
                Block dictionaryOmniBlock = new DictionaryOmniBlock(
                    (Vec)dicBlock.getValues(),
                    ((DictionaryBlock) block).getIdsArray());
                dicBlock.close();
                return dictionaryOmniBlock;
            }
            case "RunLengthEncodedBlock": {
                return buildOffHeapBlock(vecAllocator, block,
                    ((RunLengthEncodedBlock) block).getValue().getClass().getSimpleName(), block.getPositionCount());
            }
            case "LazyBlock": {
                return buildOffHeapBlock(vecAllocator, block.getLoadedBlock());
            }
            case "RowBlock": {
                RowBlock rowBlock = (RowBlock) block;
                for (int j = 0; j < positionCount; j++) {
                    if (rowBlock.isNull(j)) {
                        valueIsNull[j] = true;
                    }
                }
                return RowOmniBlock.fromFieldBlocks(rowBlock.getPositionCount(), Optional.of(valueIsNull),
                    rowBlock.getRawFieldBlocks());
            }
            default:
                break;
        }
        return null;
    }

    private static VariableWidthOmniBlock getVariableWidthOmniBlock(VecAllocator vecAllocator, Block block,
        int positionCount, boolean[] valueIsNull) {
        if (block instanceof RunLengthEncodedBlock) {
            VariableWidthBlock variableWidthBlock = (VariableWidthBlock) ((RunLengthEncodedBlock) block).getValue();
            VarcharVec vec = new VarcharVec(variableWidthBlock.getSliceLength(0) * positionCount, positionCount);

            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    valueIsNull[i] = true;
                    vec.setNull(i);
                } else {
                    vec.set(i, (byte[]) block.get(i));
                }
            }
            return new VariableWidthOmniBlock(positionCount, vec);
        }

        int[] offsets = ((VariableWidthBlock) block).getOffsets();
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = true;
            }
        }

        int arrayOffset = 0;
        int dataLength = offsets[arrayOffset + positionCount] - offsets[arrayOffset];
        VarcharVec varcharVec = new VarcharVec(vecAllocator, dataLength, positionCount);
        Slice slice = ((VariableWidthBlock) block).getRawSlice(0);
        if (slice.hasByteArray()) {
            varcharVec.put(0, slice.byteArray(), slice.byteArrayOffset(), offsets, 0, positionCount);
        }
        varcharVec.setNulls(0, valueIsNull, 0, positionCount);
        return new VariableWidthOmniBlock(positionCount, varcharVec, offsets, Optional.ofNullable(valueIsNull));
    }

    /**
     * Build a vector by {@link Block}
     *
     * @param vecAllocator VecAllocator to create vectors
     * @param page the page
     * @param operatorName the operator name
     * @return the vec batch
     */
    public static VecBatch buildVecBatch(VecAllocator vecAllocator, Page page, String operatorName) {
        List<Vec> vecList = new ArrayList<>();

        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            if (!block.isExtensionBlock()) {
                vecList.add((Vec) OperatorUtils.buildOffHeapBlock(vecAllocator, block).getValues());

                // since we dont implement RunLengthEncodeBlock yet, so the transfer of RunLengthEncodeBlock
                // is regarded as normal at present.
                if (!(block instanceof RunLengthEncodedBlock)) {
                    log.warn("transfer the onheap pages to offheap pages in %s for %s with %s rows", operatorName,
                        block.getClass().getSimpleName(), page.getPositionCount());
                }
            } else {
                if (block instanceof LazyBlock) {
                    vecList.add(buildVecInLazyBlock((LazyBlock) block));
                } else if (block instanceof DictionaryBlock) {
                    vecList.add(buildDictionaryVec((DictionaryBlock<?>) block));
                } else if (block instanceof RowBlock) {
                    vecList.add(buildContainerVec(vecAllocator, (RowBlock) block));
                } else {
                    vecList.add((Vec) block.getValues());
                }
            }
        }

        return new VecBatch(vecList);
    }

    /**
     * This method is used to merge the buffered VecBatches together
     * into a final result VecBatch. It invokes append method defined natively
     * to perform merge operation.
     *
     * @param resultVecBatch Stores final resulting vectors
     */
    public static void merge(VecBatch resultVecBatch, List<VecBatch> vecBatchesToMerge) {
        int index = 0;
        for (Vec dest : resultVecBatch.getVectors()) {
            int offSet = 0;
            for (VecBatch batch : vecBatchesToMerge) {
                Vec src = batch.getVectors()[index];

                int positionCount = src.getSize();
                if (src instanceof DictionaryVec) {
                    appendDictionaryValues(src, dest, offSet);
                } else {
                    dest.append(src, offSet, positionCount);
                }
                offSet += positionCount;
                src.close();
            }
            index++;
        }
    }

    private static void appendDictionaryValues(Vec src, Vec dest, int offSet) {
        for (int index = 0; index < src.getSize(); index++) {
            VecType.VecTypeId id = ((DictionaryVec) src).getDictionary().getType().getId();
            switch (id) {
                case OMNI_VEC_TYPE_DICTIONARY:
                    appendDictionaryValues(((DictionaryVec) src).getDictionary(), dest, offSet);
                    break;
                case OMNI_VEC_TYPE_INT:
                    ((IntVec) dest).set(offSet + index, ((DictionaryVec) src).getInt(index));
                    break;
                case OMNI_VEC_TYPE_LONG:
                    ((LongVec) dest).set(offSet + index, ((DictionaryVec) src).getLong(index));
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    ((DoubleVec) dest).set(offSet + index, ((DictionaryVec) src).getDouble(index));
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    ((BooleanVec) dest).set(offSet + index, ((DictionaryVec) src).getBoolean(index));
                    break;
                case OMNI_VEC_TYPE_VARCHAR:
                    ((VarcharVec) dest).set(offSet + index, ((DictionaryVec) src).getBytes(index));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + src.getType());
            }
        }
    }

    private static Vec buildVecInLazyBlock(LazyBlock block) {
        if (block.getLoadedBlock() instanceof DictionaryBlock) {
            return buildDictionaryVec((DictionaryBlock<?>) block.getLoadedBlock());
        }
        return (Vec) block.getLoadedBlock().getValues();
    }

    private static Vec buildContainerVec(VecAllocator vecAllocator, RowBlock block) {
        Block[] rawFieldBlocks = block.getRawFieldBlocks();
        int numFields = rawFieldBlocks.length;
        long[] vectorAddresses = new long[numFields];
        VecType[] vecTypes = new VecType[numFields];
        for (int i = 0; i < numFields; ++i) {
            Vec vec = (Vec) rawFieldBlocks[i].getValues();
            long nativeVectorAddress = vec.getNativeVector();
            vectorAddresses[i] = nativeVectorAddress;
        }
        return new ContainerVec(vecAllocator, numFields, block.getPositionCount(), vectorAddresses, vecTypes);
    }

    private static Vec buildDictionaryVec(DictionaryBlock<?> block) {
        if (block.getDictionary() instanceof DictionaryBlock) {
            buildDictionaryVec(block);
        }
        Vec dictionary = (Vec) block.getDictionary().getValues();
        Vec vec = new DictionaryVec(dictionary, block.getIdsArray());
        dictionary.close();
        return vec;
    }
}
