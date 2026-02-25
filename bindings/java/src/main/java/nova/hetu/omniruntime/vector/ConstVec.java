/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataType.DataTypeId;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A read-only vector backed by a native ConstVector.
 * Stores a single constant value logically repeated for every row in [0, size).
 *
 * @since 2024-06-01
 */
public class ConstVec extends Vec {

    private long constValueLong;
    private byte[] constValueBytes;

    public ConstVec(long nativeVector, long nativeVectorNullBufAddr, int size, DataType dataType) {
        super(nativeVector, 0L, nativeVectorNullBufAddr, 0, size, dataType, false);
        cacheConstValue();
    }

    public ConstVec(long nativeVector, DataType dataType) {
        super(nativeVector, 0L, getValueNullsNative(nativeVector), 0,
                getSizeNative(nativeVector), dataType, false);
        cacheConstValue();
    }

    private void cacheConstValue() {
        DataTypeId typeId = getType().getId();
        switch (typeId) {
            case OMNI_VARCHAR:
            case OMNI_CHAR:
            case OMNI_VARBINARY:
                constValueBytes = getConstValueAsBytesNative(getNativeVector());
                break;
            case OMNI_DECIMAL128:
                constValueBytes = getConstDecimal128BytesNative(getNativeVector());
                break;
            default:
                constValueLong = getConstValueAsLongNative(getNativeVector());
                break;
        }
    }

    @Override
    public VecEncoding getEncoding() {
        return VecEncoding.OMNI_VEC_ENCODING_CONST;
    }

    public boolean getConstBoolean() {
        return constValueLong != 0;
    }

    public byte getConstByte() {
        return (byte) constValueLong;
    }

    public short getConstShort() {
        return (short) constValueLong;
    }

    public int getConstInt() {
        return (int) constValueLong;
    }

    public long getConstLong() {
        return constValueLong;
    }

    public float getConstFloat() {
        return Float.intBitsToFloat((int) constValueLong);
    }

    public double getConstDouble() {
        return Double.longBitsToDouble(constValueLong);
    }

    public byte[] getConstBytes() {
        return constValueBytes;
    }

    public BigInteger getConstDecimal128() {
        ByteBuffer bb = ByteBuffer.wrap(constValueBytes).order(ByteOrder.LITTLE_ENDIAN);
        long low = bb.getLong();
        long high = bb.getLong();
        return Decimal128Vec.getDecimal(new long[]{low, high});
    }

    public Vec expandToFlat() {
        int size = getSize();
        DataType dataType = getType();
        Vec vector = VecFactory.createFlatVec(size, dataType);
        vector.setNulls(0, getValuesNulls(0, size), 0, size);

        switch (dataType.getId()) {
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32: {
                int val = getConstInt();
                IntVec intVec = (IntVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        intVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DATE64:
            case OMNI_DECIMAL64:
            case OMNI_TIME64:
            case OMNI_INTERVAL_DAY_TIME:
            case OMNI_INTERVAL_MONTHS: {
                long val = getConstLong();
                LongVec longVec = (LongVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        longVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_DOUBLE: {
                double val = getConstDouble();
                DoubleVec doubleVec = (DoubleVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        doubleVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_FLOAT: {
                float val = getConstFloat();
                FloatVec floatVec = (FloatVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        floatVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_SHORT: {
                short val = getConstShort();
                ShortVec shortVec = (ShortVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        shortVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_BYTE: {
                byte val = getConstByte();
                ByteVec byteVec = (ByteVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        byteVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_BOOLEAN: {
                boolean val = getConstBoolean();
                BooleanVec boolVec = (BooleanVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        boolVec.set(i, val);
                    }
                }
                break;
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR:
            case OMNI_VARBINARY: {
                byte[] val = getConstBytes();
                VarcharVec varcharVec = (VarcharVec) vector;
                for (int i = 0; i < size; i++) {
                    if (!isNull(i)) {
                        varcharVec.set(i, val);
                    } else {
                        varcharVec.setNull(i);
                    }
                }
                break;
            }
            case OMNI_DECIMAL128: {
                ByteBuffer bb = ByteBuffer.wrap(constValueBytes).order(ByteOrder.LITTLE_ENDIAN);
                long low = bb.getLong();
                long high = bb.getLong();
                long[] decVal = new long[]{low, high};
                Decimal128Vec dec128Vec = (Decimal128Vec) vector;
                for (int i = 0; i < size; i++) {
                    if (!vector.isNull(i)) {
                        dec128Vec.set(i, decVal);
                    }
                }
                break;
            }
            default:
                throw new IllegalArgumentException(
                    "ConstVec.expandToFlat: unsupported data type " + dataType.getId());
        }
        return vector;
    }

    @Override
    public Vec slice(int start, int length) {
        throw new UnsupportedOperationException("ConstVec does not support slice");
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        throw new UnsupportedOperationException("ConstVec does not support copyPositions");
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return 0;
    }

    private static native long getConstValueAsLongNative(long nativeVector);

    private static native byte[] getConstValueAsBytesNative(long nativeVector);

    private static native byte[] getConstDecimal128BytesNative(long nativeVector);
}
