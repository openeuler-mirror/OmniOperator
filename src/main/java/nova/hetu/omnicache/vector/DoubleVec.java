package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;

/**
 * Representing a floating point number
 */
public class DoubleVec extends Vec<Double>{

    private ByteBuffer data;
    public DoubleVec(int size) {
        data = OMVectorBase.allocate(size * Double.BYTES);
    }


    /**
     * Sample use the vector
     * @param args
     */
    public static void main(String[] args) {
        DoubleVec vector1 = new DoubleVec(10);
        DoubleVec vector2 = new DoubleVec(10);

        System.out.println("return: " + vector1.get(0) + ", " + vector1.get(8) + ", " + vector1.get(16));

        vector1.mul(vector2);

        System.out.println("return: " + vector1.get(0) + ", " + vector1.get(8) + ", " + vector1.get(16));

        vector1.set(8, 100.0);
        vector1.mul(vector2);

        System.out.println("return: " + vector1.get(0) + ", " + vector1.get(8) + ", " + vector1.get(16));

    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Vec<Double> other)
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