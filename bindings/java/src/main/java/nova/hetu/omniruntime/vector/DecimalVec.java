package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public abstract class DecimalVec
        extends FixedWidthVec
{
    private static final byte[] ZEROS = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static final byte[] MINUS_ONE = new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    private final int precision;
    private final int scale;

    public DecimalVec(int size, int precision, int scale, int typeLength, VecType type)
    {
        super(size * typeLength, size, type);
        this.precision = precision;
        this.scale = scale;
        setPrecisionAndScale(precision, scale);
    }

    public DecimalVec(VecAllocator allocator, int size, int precision, int scale, int typeLength, VecType type)
    {
        super(allocator, size * typeLength, size, type);
        this.precision = precision;
        this.scale = scale;
        setPrecisionAndScale(precision, scale);
    }

    public DecimalVec(long nativeVector)
    {
        super(nativeVector);
        this.precision = getPrecision(nativeVector);
        this.scale = getScale(nativeVector);
    }

    protected DecimalVec(DecimalVec vector, int offset, int length, boolean isSlice)
    {
        super(vector, offset, length, isSlice);
        this.precision = vector.precision;
        this.scale = vector.scale;
    }

    protected DecimalVec(DecimalVec vector, int[] positions, int offset, int length)
    {
        super(vector, positions, offset, length);
        this.precision = vector.precision;
        this.scale = vector.scale;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public abstract void set(int index, BigDecimal value);

    protected void set(int index, BigDecimal value, int typeLength)
    {
        checkPrecisionAndScale(value);
        getValueNulls().set(index);
        writeBigDecimalToBuf(value, index, typeLength);
    }

    public abstract BigDecimal get(int index);

    protected BigDecimal get(int index, int typeLength)
    {
        return getBigDecimalFromBuf(index + offset, typeLength);
    }

    private BigDecimal getBigDecimalFromBuf(int index, int typeLength)
    {
        byte[] value = new byte[typeLength];
        final int startIndex = index * typeLength;

        getValues().position(startIndex);
        getValues().get(value, 0, typeLength);

        // TODO:default is little endian and need check bytes order
        reverse(value);
        BigInteger unscaledValue = new BigInteger(value);
        return new BigDecimal(unscaledValue, scale, new MathContext(precision));
    }

    private void reverse(byte[] values)
    {
        int length = values.length;
        byte temp;
        for (int i = 0; i < length / 2; i++) {
            temp = values[i];
            values[i] = values[length - 1 - i];
            values[length - 1 - i] = temp;
        }
    }

    private void writeBigDecimalToBuf(BigDecimal value, int index, int typeLength)
    {
        final byte[] bytes = value.unscaledValue().toByteArray();
        final int startIndex = index * typeLength;
        if (bytes.length > typeLength) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT,
                    String.format("Decimal size greater then %d bytes:%d", typeLength, bytes.length));
        }
        byte[] padBytes = bytes[0] < 0 ? MINUS_ONE : ZEROS;
        // TODO:default is little endian and need check bytes order
        reverse(bytes);
        // Write LE data
        getValues().position(startIndex);
        getValues().put(bytes, 0, bytes.length);
        getValues().position(startIndex + bytes.length);
        getValues().put(padBytes, 0, typeLength - bytes.length);
    }

    private void checkPrecisionAndScale(BigDecimal value)
    {
        if (value.precision() > precision) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT,
                    String.format("BigDecimal precision can not be greater than decimal vector:%d != %d", value.precision(), precision));
        }

        if (value.scale() != scale) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT,
                    String.format("BigDecimal scale must equal than in the decimal vector:%d != %d", value.scale(), scale));
        }
    }

    private static native void setPrecisionAndScale(int precision, int scale);

    private static native int getPrecision(long nativeVector);

    private static native int getScale(long nativeVector);
}
