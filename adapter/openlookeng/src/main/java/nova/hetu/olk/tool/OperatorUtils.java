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
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
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

    private static Block getDictionary(DictionaryVec vec) {
        Vec dictionary = vec.getDictionary();
        VecType vecType = dictionary.getType();
        int positionCount = dictionary.getSize();

        switch (vecType.getId()) {
            case OMNI_VEC_TYPE_INT:
                return new IntArrayOmniBlock(positionCount, Optional.empty(), (IntVec) dictionary);
            case OMNI_VEC_TYPE_LONG:
                return new LongArrayOmniBlock(positionCount, Optional.empty(), (LongVec) dictionary);
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + vecType);
        }
    }

    /**
     * Transfer to off heap pages list.
     *
     * @param pages the pages
     * @return the list
     */
    public static List<Page> transferToOffHeapPages(List<Page> pages) {
        List<Page> offHeapInput = new ArrayList<>();
        for (Page page : pages) {
            Block[] blocks = getOffHeapBlocks(page.getBlocks());
            offHeapInput.add(new Page(blocks));
        }
        return offHeapInput;
    }

    /**
     * Transfer to off heap pages page.
     *
     * @param page the page
     * @return the page
     */
    public static Page transferToOffHeapPages(Page page) {
        Block[] blocks = getOffHeapBlocks(page.getBlocks());
        return new Page(blocks);
    }

    private static Block[] getOffHeapBlocks(Block[] blocks) {
        Block[] res = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            res[i] = getOffHeapBlock(blocks[i]);
        }
        return res;
    }

    /**
     * Gets off heap block.
     *
     * @param block the block
     * @return the off heap block
     */
    public static Block getOffHeapBlock(Block block) {
        if (block.isExtensionBlock()) {
            return block;
        }
        int positionCount = block.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount];
        switch (block.getClass().getSimpleName()) {
            case "IntArrayBlock": {
                int[] ints = new int[positionCount];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        ints[j] = (int) block.get(j);
                    }
                }
                return new IntArrayOmniBlock(positionCount, Optional.of(valueIsNull), ints);
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
                return new LongArrayOmniBlock(positionCount, Optional.of(valueIsNull), longs);
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
                return new DoubleArrayOmniBlock(positionCount, Optional.of(valueIsNull), doubles);
            }
            case "Int128ArrayBlock": {
                long[] longs = new long[positionCount];
                for (int j = 0; j < positionCount; j++) {
                    if (block.isNull(j)) {
                        valueIsNull[j] = true;
                    } else {
                        longs[j] = (long) block.get(j);
                    }
                }
                return new Int128ArrayOmniBlock(positionCount, Optional.of(valueIsNull), longs);
            }
            case "VariableWidthBlock": {
                return getVariableWidthOmniBlock(block, positionCount, valueIsNull);
            }
            case "DictionaryBlock": {
                return new DictionaryOmniBlock(getOffHeapBlock(((DictionaryBlock) block).getDictionary()),
                    ((DictionaryBlock) block).getIdsArray());
            }
            case "LazyBlock": {
                return ((LazyBlock) block).getBlock();
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

    private static VariableWidthOmniBlock getVariableWidthOmniBlock(Block block, int positionCount,
        boolean[] valueIsNull) {
        int[] offsets = ((VariableWidthBlock) block).getOffsets();
        for (int j = 0; j < positionCount; j++) {
            if (block.isNull(j)) {
                valueIsNull[j] = true;
            }
        }

        int arrayOffset = 0;
        int dataLength = offsets[arrayOffset + positionCount] - offsets[arrayOffset];
        VarcharVec varcharVec = new VarcharVec(dataLength, positionCount);
        Slice slice = ((VariableWidthBlock) block).getRawSlice(0);
        if (slice.hasByteArray()) {
            varcharVec.put(0, slice.byteArray(), slice.byteArrayOffset(), offsets, 0, positionCount);
        }
        varcharVec.setNulls(0, valueIsNull, 0, positionCount);
        return new VariableWidthOmniBlock(positionCount, varcharVec, offsets, Optional.ofNullable(valueIsNull));
    }

    /**
     * Gets vec batch.
     *
     * @param page the page
     * @param operatorName the operator name
     * @return the vec batch
     */
    public static VecBatch getVecBatch(Page page, String operatorName) {
        List<Vec> vecList = new ArrayList<>();

        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            if (!block.isExtensionBlock()) {
                vecList.add((Vec) OperatorUtils.getOffHeapBlock(block).getValues());
                log.warn("transfer the onheap pages to offheap pages in %s with %s rows", operatorName,
                    page.getPositionCount());
            } else {
                if (block instanceof LazyBlock) {
                    vecList.add(getVecInLazyBlock((LazyBlock) block));
                } else if (block instanceof DictionaryBlock) {
                    vecList.add(getDictionaryVec((DictionaryBlock<?>) block));
                } else if (block instanceof RowBlock) {
                    vecList.add(getContainerVec((RowBlock) block));
                } else {
                    vecList.add((Vec) block.getValues());
                }
            }
        }

        VecBatch vecBatch = new VecBatch(vecList);
        return vecBatch;
    }

    private static Vec getVecInLazyBlock(LazyBlock block) {
        if (block.getLoadedBlock() instanceof DictionaryBlock) {
            return getDictionaryVec((DictionaryBlock<?>) block.getLoadedBlock());
        }
        return (Vec) block.getLoadedBlock().getValues();
    }

    private static Vec getContainerVec(RowBlock block) {
        Block[] rawFieldBlocks = block.getRawFieldBlocks();
        int numFields = rawFieldBlocks.length;
        long[] vectorAddresses = new long[numFields];
        VecType[] vecTypes = new VecType[numFields];
        for (int i = 0; i < numFields; ++i) {
            Vec vec = (Vec) rawFieldBlocks[i].getValues();
            long nativeVectorAddress = vec.getNativeVector();
            vectorAddresses[i] = nativeVectorAddress;
        }
        return new ContainerVec(numFields, block.getPositionCount(), vectorAddresses, vecTypes);
    }

    private static Vec getDictionaryVec(DictionaryBlock<?> block) {
        if (block.getDictionary() instanceof DictionaryBlock) {
            getDictionaryVec(block);
        }
        return new DictionaryVec((Vec) block.getDictionary().getValues(), block.getIdsArray());
    }
}
