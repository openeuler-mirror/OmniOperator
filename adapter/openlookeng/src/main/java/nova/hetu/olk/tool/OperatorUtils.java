/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;

import com.google.common.primitives.Ints;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.AbstractVariableWidthBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.ContainerDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
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
     * convert type [] to data type [ ].
     *
     * @param types the types
     * @return the data type [ ]
     */
    public static DataType[] toDataTypes(List<? extends Type> types) {
        DataType[] dataTypes = types.stream().map(OperatorUtils::toDataType).toArray(DataType[]::new);
        return dataTypes;
    }

    /**
     * convert type to data type.
     *
     * @param type the type
     * @return the data type
     */
    public static DataType toDataType(Type type) {
        TypeSignature signature = type.getTypeSignature();
        String base = signature.getBase();
        switch (base) {
            case StandardTypes.INTEGER:
                return IntDataType.INTEGER;
            case StandardTypes.BIGINT:
                return LongDataType.LONG;
            case StandardTypes.DOUBLE:
                return DoubleDataType.DOUBLE;
            case StandardTypes.BOOLEAN:
                return BooleanDataType.BOOLEAN;
            case StandardTypes.VARBINARY:
                return new VarcharDataType(0);
            case StandardTypes.VARCHAR:
                int width = signature.getParameters().get(0).getLongLiteral().intValue();
                return new VarcharDataType(width);
            case StandardTypes.CHAR:
                return new CharDataType(signature.getParameters().get(0).getLongLiteral().intValue());
            case StandardTypes.DECIMAL:
                int precision = signature.getParameters().get(0).getLongLiteral().intValue();
                int scale = signature.getParameters().get(1).getLongLiteral().intValue();
                if (precision <= MAX_SHORT_PRECISION) {
                    return new Decimal64DataType(precision, scale);
                }
                return new Decimal128DataType(precision, scale);
            case StandardTypes.DATE:
                return Date32DataType.DATE32;
            case StandardTypes.ROW:
                RowType rowType = (RowType) type;
                return new ContainerDataType(toDataTypes(rowType.getTypeParameters()));
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
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
     * @param dataTypes data types
     * @param totalPositions Size for all the vectors
     * @return List contains blank vectors
     */
    public static List<Vec> createBlankVectors(VecAllocator vecAllocator, DataType[] dataTypes, int totalPositions) {
        List<Vec> vecsResult = new ArrayList<>();
        for (int i = 0; i < dataTypes.length; i++) {
            DataType type = dataTypes[i];
            switch (type.getId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                    vecsResult.add(new IntVec(vecAllocator, totalPositions));
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    vecsResult.add(new LongVec(vecAllocator, totalPositions));
                    break;
                case OMNI_DOUBLE:
                    vecsResult.add(new DoubleVec(vecAllocator, totalPositions));
                    break;
                case OMNI_BOOLEAN:
                    vecsResult.add(new BooleanVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    vecsResult.add(new VarcharVec(vecAllocator, totalPositions * ((VarcharDataType) type).getWidth(),
                            totalPositions));
                    break;
                case OMNI_DECIMAL128:
                    vecsResult.add(new Decimal128Vec(vecAllocator, totalPositions));
                    break;
                case OMNI_CONTAINER:
                    vecsResult.add(createBlankContainerVector(vecAllocator, type, totalPositions));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data type " + type);
            }
        }
        return vecsResult;
    }

    private static ContainerVec createBlankContainerVector(VecAllocator vecAllocator, DataType type,
            int totalPositions) {
        if (!(type instanceof ContainerDataType)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "type is not container type:" + type);
        }
        ContainerDataType containerDataType = (ContainerDataType) type;
        List<Vec> fieldVecs = createBlankVectors(vecAllocator, containerDataType.getFieldTypes(), totalPositions);
        long[] nativeVec = new long[fieldVecs.size()];
        for (int i = 0; i < fieldVecs.size(); i++) {
            nativeVec[i] = fieldVecs.get(i).getNativeVector();
        }
        return new ContainerVec(vecAllocator, containerDataType.size(), totalPositions, nativeVec,
                containerDataType.getFieldTypes());
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

    private static Block buildByteArrayOmniBlock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        byte[] bytes = new byte[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            } else {
                bytes[j] = (byte) block.get(j);
            }
        }
        return new ByteArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), bytes);
    }

    private static Block buildIntArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        int[] ints = new int[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            } else {
                ints[j] = (int) block.get(j);
            }
        }
        return new IntArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), ints);
    }

    private static Block buildLongArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        long[] longs = new long[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            } else {
                longs[j] = (long) block.get(j);
            }
        }
        return new LongArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), longs);
    }

    private static Block buildDoubleArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        double[] doubles = new double[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            } else {
                doubles[j] = (double) block.get(j);
            }
        }
        return new DoubleArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), doubles);
    }

    private static Block buildInt128ArrayOmniBlock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        long[] longs = new long[positionCount * 2];
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            } else {
                long[] data = (long[]) block.get(j);
                longs[j * 2] = data[0];
                longs[j * 2 + 1] = data[1];
            }
        }
        return new Int128ArrayOmniBlock(vecAllocator, positionCount, Optional.of(valueIsNull), longs);
    }

    private static VariableWidthOmniBlock buildVariableWidthOmniBlock(VecAllocator vecAllocator, Block block,
            int positionCount, boolean isRLE) {
        byte[] valueIsNull = new byte[positionCount];
        if (!isRLE) {
            int[] offsets = ((VariableWidthBlock) block).getOffsets();
            for (int j = 0; j < positionCount; j++) {
                if (block.isNull(j)) {
                    valueIsNull[j] = Vec.NULL;
                }
            }
            Slice slice = ((VariableWidthBlock) block).getRawSlice(0);
            return new VariableWidthOmniBlock(vecAllocator, positionCount, slice, offsets,
                    Optional.ofNullable(valueIsNull));
        } else {
            AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) ((RunLengthEncodedBlock) block)
                    .getValue();
            VarcharVec vec = new VarcharVec(vecAllocator, variableWidthBlock.getSliceLength(0) * positionCount,
                    positionCount);
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    valueIsNull[i] = Vec.NULL;
                    vec.setNull(i);
                } else {
                    vec.set(i, (byte[]) block.get(i));
                }
            }
            return new VariableWidthOmniBlock(positionCount, vec);
        }
    }

    private static Block buildDictionaryOmniBlock(VecAllocator vecAllocator, Block block) {
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
        Block dictionary = buildOffHeapBlock(vecAllocator, dictionaryBlock.getDictionary());
        Block dictionaryOmniBlock = new DictionaryOmniBlock((Vec) dictionary.getValues(),
                dictionaryBlock.getIdsArray());
        dictionary.close();
        return dictionaryOmniBlock;
    }

    private static Block buildRowOmniBlock(VecAllocator vecAllocator, Block block, int positionCount) {
        byte[] valueIsNull = new byte[positionCount];
        RowBlock rowBlock = (RowBlock) block;
        for (int j = 0; j < positionCount; j++) {
            if (rowBlock.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            }
        }
        return RowOmniBlock.fromFieldBlocks(vecAllocator, rowBlock.getPositionCount(), Optional.of(valueIsNull),
                rowBlock.getRawFieldBlocks());
    }

    /**
     * Gets off heap block.
     *
     * @param vecAllocator vector allocator
     * @param block the block
     * @param type the actual block type, e.g. RunLengthEncodedBlock or
     *            DictionaryBlock
     * @param positionCount the position count of the block
     * @return the off heap block
     */
    public static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block, String type, int positionCount) {
        return buildOffHeapBlock(vecAllocator, block, type, positionCount, false);
    }

    private static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block, String type, int positionCount,
            boolean isRLE) {
        if (block.isExtensionBlock()) {
            return block;
        }

        switch (type) {
            case "ByteArrayBlock" :
            case "ByteArrayOmniBlock" :
                return buildByteArrayOmniBlock(vecAllocator, block, positionCount);
            case "IntArrayBlock" :
            case "IntArrayOmniBlock" :
                return buildIntArrayOmniBLock(vecAllocator, block, positionCount);
            case "LongArrayBlock" :
            case "LongArrayOmniBlock" :
                return buildLongArrayOmniBLock(vecAllocator, block, positionCount);
            case "DoubleArrayBlock" :
            case "DoubleArrayOmniBlock" :
                return buildDoubleArrayOmniBLock(vecAllocator, block, positionCount);
            case "Int128ArrayBlock" :
            case "Int128ArrayOmniBlock" :
                return buildInt128ArrayOmniBlock(vecAllocator, block, positionCount);
            case "VariableWidthBlock" :
            case "VariableWidthOmniBlock" :
                return buildVariableWidthOmniBlock(vecAllocator, block, positionCount, isRLE);
            case "DictionaryBlock" :
                return buildDictionaryOmniBlock(vecAllocator, block);
            case "RunLengthEncodedBlock" :
                return buildOffHeapBlock(vecAllocator, block,
                        ((RunLengthEncodedBlock) block).getValue().getClass().getSimpleName(), positionCount, true);
            case "LazyBlock" :
                return new LazyOmniBlock(vecAllocator, (LazyBlock) block);
            case "RowBlock" :
                return buildRowOmniBlock(vecAllocator, block, positionCount);
            default :
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support block:" + type);
        }
    }

    /**
     * Build a vector from block.
     *
     * @param vecAllocator vector allocator.
     * @param block block
     * @return vector instance.
     */
    public static Vec buildVec(VecAllocator vecAllocator, Block block) {
        if (!block.isExtensionBlock()) {
            return (Vec) OperatorUtils.buildOffHeapBlock(vecAllocator, block).getValues();
        } else {
            return (Vec) block.getValues();
        }
    }

    /**
     * Build a vector by {@link Block}
     *
     * @param vecAllocator VecAllocator to create vectors
     * @param page the page
     * @param object the operator
     * @return the vec batch
     */
    public static VecBatch buildVecBatch(VecAllocator vecAllocator, Page page, Object object) {
        List<Vec> vecList = new ArrayList<>();

        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            Vec vec = buildVec(vecAllocator, block);
            vecList.add(vec);
        }

        return new VecBatch(vecList, page.getPositionCount());
    }

    /**
     * This method is used to merge the buffered VecBatches together into a final
     * result VecBatch. It invokes append method defined natively to perform merge
     * operation.
     *
     * @param resultVecBatch Stores final resulting vectors
     */
    public static void merge(VecBatch resultVecBatch, List<Page> pages, VecAllocator vecAllocator) {
        for (int channel = 0; channel < resultVecBatch.getVectorCount(); channel++) {
            int offset = 0;
            for (Page page : pages) {
                int positionCount = page.getPositionCount();
                Block block = page.getBlock(channel);
                Vec src;
                if (!block.isExtensionBlock()) {
                    block = OperatorUtils.buildOffHeapBlock(vecAllocator, block);
                }
                src = (Vec) block.getValues();
                Vec dest = resultVecBatch.getVector(channel);
                dest.append(src, offset, positionCount);

                offset += positionCount;
                src.close();
            }
        }
    }

    /**
     * build a RowOmniBlock from a ContainerVec
     *
     * @param containerVec a container vector
     * @return the RowOmniBlock
     */
    public static RowOmniBlock buildRowOmniBlock(ContainerVec containerVec) {
        DataType[] dataTypes = containerVec.getDataTypes();
        int positionCount = containerVec.getPositionCount();
        Block[] rowBlocks = new Block[dataTypes.length];
        int vectorCount = containerVec.getDataTypes().length;
        for (int vecIdx = 0; vecIdx < vectorCount; ++vecIdx) {
            DataType dataType = dataTypes[vecIdx];
            switch (dataType.getId()) {
                case OMNI_BOOLEAN:
                    rowBlocks[vecIdx] = new ByteArrayOmniBlock(positionCount,
                            new BooleanVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    rowBlocks[vecIdx] = new IntArrayOmniBlock(positionCount,
                            new IntVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    rowBlocks[vecIdx] = new LongArrayOmniBlock(positionCount,
                            new LongVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_DOUBLE:
                    rowBlocks[vecIdx] = new DoubleArrayOmniBlock(positionCount,
                            new DoubleVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    rowBlocks[vecIdx] = new VariableWidthOmniBlock(positionCount,
                            new VarcharVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_DECIMAL128:
                    rowBlocks[vecIdx] = new Int128ArrayOmniBlock(positionCount,
                            new Decimal128Vec(containerVec.getVector(vecIdx), dataType));
                    break;
                default:
                    throw new PrestoException(GENERIC_INTERNAL_ERROR,
                            "Unsupported data type " + dataTypes[vecIdx].getId());
            }
        }
        int[] fieldBlockOffsets = new int[positionCount + 1];
        byte[] nulls = containerVec.getRawValueNulls();
        for (int position = 0; position < positionCount; position++) {
            fieldBlockOffsets[position + 1] = fieldBlockOffsets[position] + (nulls[position] == Vec.NULL ? 0 : 1);
        }
        return new RowOmniBlock(0, positionCount, nulls, fieldBlockOffsets, rowBlocks);
    }
}
