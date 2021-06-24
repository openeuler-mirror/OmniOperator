package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.math.BigDecimal;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_128_DECIMAL;

public class Decimal128Vec
        extends DecimalVec
{
    private static final int BYTES = Long.BYTES * 2;

    public Decimal128Vec(int size, int precision, int scale)
    {
        super(size, precision, scale, BYTES, OMNI_VEC_TYPE_128_DECIMAL);
    }

    public Decimal128Vec(VecAllocator allocator, int size, int precision, int scale)
    {
        super(allocator, size, precision, scale, BYTES, OMNI_VEC_TYPE_128_DECIMAL);
    }

    protected Decimal128Vec(long nativeVector)
    {
        super(nativeVector);
    }

    private Decimal128Vec(Decimal128Vec vector, int offset, int length)
    {
        super(vector, offset, length);
    }

    @Override
    public void set(int index, BigDecimal value)
    {
        set(index, value, BYTES);
    }

    @Override
    public BigDecimal get(int index)
    {
        return get(index, BYTES);
    }

    @Override
    public Decimal128Vec slice(int start, int end)
    {
        return new Decimal128Vec(this, start, end - start);
    }

    @Override
    public Decimal128Vec copy()
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Decimal128Vec is not supported");
    }
}
