package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.math.BigDecimal;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_256_DECIMAL;

public class Decimal256Vec
        extends DecimalVec
{
    private static final int BYTES = Long.BYTES * 4;

    public Decimal256Vec(int size, int precision, int scale)
    {
        super(size, precision, scale, BYTES, OMNI_VEC_TYPE_256_DECIMAL);
    }

    public Decimal256Vec(VecAllocator allocator, int size, int precision, int scale)
    {
        super(allocator, size, precision, scale, BYTES, OMNI_VEC_TYPE_256_DECIMAL);
    }

    protected Decimal256Vec(long nativeVector)
    {
        super(nativeVector);
    }

    private Decimal256Vec(Decimal256Vec vector, int offset, int length)
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
    public Decimal256Vec slice(int start, int end)
    {
        return new Decimal256Vec(this, start, end - start);
    }

    @Override
    public Decimal256Vec copy()
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Decimal256Vec is not supported");
    }
}
