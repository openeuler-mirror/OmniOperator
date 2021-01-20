package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;

/**
 * Representing a floating point number
 */
public class DoubleVec extends Vec<Double>{

    private OMVectorBase base = new OMVectorBase();

    public DoubleVec(int size) {
        super(size * Double.BYTES);
        this.size = size;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Double other)
    {
        return null;
    }

    @Override
    public Vec mmul(Vec<Double> other)
    {
        return null;
    }

    @Override
    public Vec filter()
    {
        return null;
    }

    @Override
    public void set(int idx, Double value)
    {
        data.putDouble(idx * Double.BYTES, value);
    }

    @Override
    public Double get(int idx)
    {
        return data.getDouble(idx * Double.BYTES);
    }

    @Override
    public Vec groupby()
    {
        return null;
    }

    @Override
    public Vec join(Vec other)
    {
        return null;
    }
}