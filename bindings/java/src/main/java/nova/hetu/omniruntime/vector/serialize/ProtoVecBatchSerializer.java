/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector.serialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;

import nova.hetu.omniruntime.type.CharVecType;
import nova.hetu.omniruntime.type.Date32VecType;
import nova.hetu.omniruntime.type.Date64VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.Decimal64VecType;
import nova.hetu.omniruntime.type.VarcharVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.JvmUtils;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.VariableWidthVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.vector.VecFactory;

/**
 * VecBatchSerializer implementation of protobuf
 *
 * @since 2021-09-13
 */
public class ProtoVecBatchSerializer implements VecBatchSerializer {
    // TODO: too much data copy, need to be optimized.
    @Override
    public byte[] serialize(VecBatch vecBatch) {
        VecBatchSerde.VecBatch.Builder vecBatchBuilder = VecBatchSerde.VecBatch.newBuilder();

        // set vectors
        int index = 0;
        for (Vec vector : vecBatch.getVectors()) {
            VecBatchSerde.Vec vec = buildProtoVec(vector, null);
            vecBatchBuilder.addVectors(index, vec);
            index++;
        }

        return vecBatchBuilder.setRowCount(vecBatch.getRowCount())
            .setVecCount(vecBatch.getVectorCount())
            .build()
            .toByteArray();
    }

    private VecBatchSerde.Vec buildProtoVec(Vec vec, int[] ids) {
        VecBatchSerde.Vec.Builder protoVecBuilder = VecBatchSerde.Vec.newBuilder();
        VecBatchSerde.VecTypeExt.Builder protoVecTypeExtBuild = VecBatchSerde.VecTypeExt.newBuilder();
        VecType type = vec.getType();
        protoVecTypeExtBuild.setId(VecBatchSerde.VecTypeExt.VecTypeId.valueOf(type.getId().name()));
        switch (type.getId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_SHORT:
            case OMNI_VEC_TYPE_BOOLEAN:
            case OMNI_VEC_TYPE_DOUBLE:
                break;
            case OMNI_VEC_TYPE_DATE32:
                protoVecTypeExtBuild.setDateUnit(
                    VecBatchSerde.VecTypeExt.DateUnit.valueOf(((Date32VecType) type).getDateUnit().name()));
                break;
            case OMNI_VEC_TYPE_DATE64:
                protoVecTypeExtBuild.setDateUnit(
                    VecBatchSerde.VecTypeExt.DateUnit.valueOf(((Date64VecType) type).getDateUnit().name()));
                break;
            case OMNI_VEC_TYPE_VARCHAR:
                protoVecTypeExtBuild.setWidth(((VarcharVecType) type).getWidth());
                break;
            case OMNI_VEC_TYPE_CHAR:
                protoVecTypeExtBuild.setWidth(((CharVecType) type).getWidth());
                break;
            case OMNI_VEC_TYPE_INTERVAL_DAY_TIME:
                protoVecTypeExtBuild.setDateUnit(VecBatchSerde.VecTypeExt.DateUnit.DAY);
                break;
            case OMNI_VEC_TYPE_INTERVAL_MONTHS:
                protoVecTypeExtBuild.setDateUnit(VecBatchSerde.VecTypeExt.DateUnit.MILLI);
                break;
            case OMNI_VEC_TYPE_DECIMAL64: {
                if (type instanceof Decimal64VecType) {
                    protoVecTypeExtBuild.setScale(((Decimal64VecType) type).getScale());
                    protoVecTypeExtBuild.setPrecision(((Decimal64VecType) type).getPrecision());
                } else {
                    throw new IllegalStateException("Unexpected value: " + type.getId());
                }
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                if (type instanceof Decimal128VecType) {
                    protoVecTypeExtBuild.setScale(((Decimal128VecType) type).getScale());
                    protoVecTypeExtBuild.setPrecision(((Decimal128VecType) type).getPrecision());
                } else {
                    throw new IllegalStateException("Unexpected value: " + type.getId());
                }
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                DictionaryVec dictionaryVec = (DictionaryVec) vec;
                int[] preIds = dictionaryVec.getIds();
                int[] nowIds;
                if (ids != null) {
                    nowIds = new int[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        nowIds[i] = preIds[ids[i]];
                    }
                } else {
                    nowIds = preIds;
                }
                return buildProtoVec(dictionaryVec.getDictionary(), nowIds);
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                ContainerVec containerVec = (ContainerVec) vec;
                int vecCount = containerVec.getVecTypes().length;
                VecType[] subVecTypes = containerVec.getVecTypes();
                for (int i = 0; i < vecCount; i++) {
                    Vec subVec = VecFactory.create(containerVec.getVector(i), subVecTypes[i]);
                    VecBatchSerde.Vec subProtoVec = buildProtoVec(subVec, null);
                    protoVecBuilder.addSubVectors(subProtoVec);
                }
                break;
            }
            // TODO: support time32 and time64
            case OMNI_VEC_TYPE_TIME32:
            case OMNI_VEC_TYPE_TIME64:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type.getId());
        }

        Vec compactVec = compactVec(vec, ids);

        if (compactVec instanceof VariableWidthVec) {
            VariableWidthVec variableWidthVec = (VariableWidthVec) compactVec;
            ByteBuffer buffer = JvmUtils.directBuffer(variableWidthVec.getOffsetsBuf());
            // only serialize the actual offset size
            buffer.limit(variableWidthVec.getRealOffsetBufCapacityInBytes());
            protoVecBuilder.setOffsets(ByteString.copyFrom(buffer));
        }

        ByteBuffer valueBuf = JvmUtils.directBuffer(compactVec.getValuesBuf());
        // only serialize the data actually written
        valueBuf.limit(compactVec.getRealValueBufCapacityInBytes());
        ByteBuffer valueNullsBuf = JvmUtils.directBuffer(compactVec.getValueNullsBuf());
        // only serialize the actual null size
        valueNullsBuf.limit(compactVec.getRealNullBufCapacityInBytes());
        VecBatchSerde.Vec protoVec = protoVecBuilder.setTypeExt(protoVecTypeExtBuild.build())
            .setSize(compactVec.getSize())
            .setOffset(compactVec.getOffset())
            .setValues(ByteString.copyFrom(valueBuf))
            .setNulls(ByteString.copyFrom(valueNullsBuf))
            .build();

        if (compactVec != vec) {
            compactVec.close();
        }
        return protoVec;
    }

    private Vec compactVec(Vec vec, int[] ids) {
        // original vec is dictionary vec
        if (ids != null) {
            return vec.copyPositions(ids, 0, ids.length);
        }
        // original vec slice
        if (vec.getOffset() != 0) {
            return vec.copyRegion(0, vec.getSize());
        }
        return vec;
    }

    @Override
    public VecBatch deserialize(byte[] bytes) {
        try {
            VecBatchSerde.VecBatch protoVecBatch = VecBatchSerde.VecBatch.parseFrom(bytes);
            int vecCount = protoVecBatch.getVecCount();
            int rowCount = protoVecBatch.getRowCount();
            Vec[] vecs = new Vec[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, protoVecBatch.getVectors(i));
            }
            return new VecBatch(vecs, rowCount);
        } catch (InvalidProtocolBufferException e) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_INNER_ERROR, "deserialize failed." + e.getCause());
        }
    }

    @Override
    public VecBatch deserialize(VecAllocator vecAllocator, byte[] bytes) {
        try {
            VecBatchSerde.VecBatch protoVecBatch = VecBatchSerde.VecBatch.parseFrom(bytes);
            int vecCount = protoVecBatch.getVecCount();
            int rowCount = protoVecBatch.getRowCount();
            Vec[] vecs = new Vec[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(vecAllocator, protoVecBatch.getVectors(i));
            }
            return new VecBatch(vecs, rowCount);
        } catch (InvalidProtocolBufferException e) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_INNER_ERROR, "deserialize failed." + e.getCause());
        }
    }

    private Vec buildVec(VecAllocator vecAllocator, VecBatchSerde.Vec protoVec) {
        VecBatchSerde.VecTypeExt protoTypeExt = protoVec.getTypeExt();
        int vecSize = protoVec.getSize();
        Vec vec;
        switch (protoTypeExt.getId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                vec = new IntVec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DATE64:
            case OMNI_VEC_TYPE_DECIMAL64:
                vec = new LongVec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_SHORT:
                vec = new ShortVec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                vec = new BooleanVec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                vec = new DoubleVec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                vec = new VarcharVec(vecAllocator, protoVec.getValues().size(), protoVec.getSize());
                if (vec instanceof VarcharVec) {
                    ((VarcharVec) vec).setOffsetsBuf(protoVec.getOffsets().toByteArray());
                }
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                vec = new Decimal128Vec(vecAllocator, vecSize);
                break;
            case OMNI_VEC_TYPE_CONTAINER:
                int vecCount = protoVec.getSubVectorsCount();
                long[] subVecAddresses = new long[vecCount];
                VecType[] subVecTypes = new VecType[vecCount];
                for (int i = 0; i < vecCount; i++) {
                    Vec subVec = buildVec(vecAllocator, protoVec.getSubVectors(i));
                    subVecAddresses[i] = subVec.getNativeVector();
                    subVecTypes[i] = subVec.getType();
                }
                return new ContainerVec(vecAllocator, vecCount, protoVec.getSize(), subVecAddresses, subVecTypes);
            // TODO: support other data types
            case OMNI_VEC_TYPE_TIME32:
            case OMNI_VEC_TYPE_TIME64:
            case OMNI_VEC_TYPE_INTERVAL_DAY_TIME:
            case OMNI_VEC_TYPE_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + protoTypeExt.getId());
        }
        vec.setValuesBuf(protoVec.getValues().toByteArray());
        vec.setNullsBuf(protoVec.getNulls().toByteArray());
        return vec;
    }
}
