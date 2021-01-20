package nova.hetu.omnicache.vector;

import org.testng.annotations.Test;

public class VectorTest
{

    @Test
    public void testIntMul()
    {
        IntVec vec1 = new IntVec(10);
        IntVec vec2 = new IntVec(10);
        for (int i=0; i<vec1.size(); i++) {
            System.out.println(i + "=" + vec1.get(i));
            vec1.set(i, i);
        }

        vec1.mul(10);

        for (int i=0; i<vec1.size(); i++) {
            System.out.println(i + "=" + vec1.get(i));
        }
    }

    @Test
    public void testLongMul()
    {
        LongVec vec1 = new LongVec(10);
        LongVec vec2 = new LongVec(10);
        for (int i=0; i<vec1.size(); i++) {
            System.out.println(i + "=" + vec1.get(i));
            vec1.set(i, (long)i);
        }

        vec1.mul((long)10);

        for (int i=0; i<vec1.size(); i++) {
            System.out.println(i + "=" + vec1.get(i));
        }
    }

}