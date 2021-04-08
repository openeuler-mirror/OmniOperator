package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.Vec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class OmniOrderByTest
{
    OmniOrderBy orderBy;
    @BeforeClass
    public void setUp() {
        orderBy = new OmniOrderBy();
    }

    @Test
    public void testOrderByOneColumn()
    {
        IntVec vec = new IntVec(8);
        vec.set(0, 5);
        vec.set(1, 3);
        vec.set(2, 2);
        vec.set(3, 6);
        vec.set(4, 1);
        vec.set(5, 4);
        vec.set(6, 7);
        vec.set(7, 8);

        List<Vec> datas = new ArrayList<>();
        datas.add(vec);
        List<Vec> nulls = new ArrayList<>();
        IntVec nullVec = new IntVec(8);
        nulls.add(nullVec);

        int[] sourceTypes = {1};
        int[] outputCols = {0};
        int[] sortCols = {0};
        int[] ascendings = {1};
        int[] nullFirsts = {0};

        long contextAddress = 0;
        long sortAddress = orderBy.createOperator(contextAddress, sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        orderBy.addInput(contextAddress, sortAddress, datas, nulls, 1, 1);
        orderBy.execute(contextAddress, sortAddress);
        OMResult result = orderBy.getOutput(contextAddress, sortAddress);

        ByteBuffer[] output = result.getBuffers();
        int len = result.getLength();
        int[] actual = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual[i] = output[0].getInt(i * Integer.BYTES);
        }
        int[] expectd = {1, 2, 3, 4, 5, 6, 7, 8};
        Assert.assertEquals(actual, expectd);

        contextAddress = orderBy.prepare(sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        sortAddress = orderBy.createOperator(contextAddress, sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        orderBy.addInput(contextAddress, sortAddress, datas, nulls, 1, 1);
        orderBy.execute(contextAddress, sortAddress);
        result = orderBy.getOutput(contextAddress, sortAddress);

        output = result.getBuffers();
        len = result.getLength();
        actual = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual[i] = output[0].getInt(i * Integer.BYTES);
        }
        Assert.assertEquals(actual, expectd);
    }

    @Test
    public void testOrderByPerformance()
    {
        long start = System.currentTimeMillis();
        List<Vec> vecs = buildVecs();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("buildVecs elapsed time : " + elapsed + " ms");

        int[] sourceTypes = {1, 1};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};

        long contextAddress = orderBy.prepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

        start = System.currentTimeMillis();
        long sortAddress = orderBy.createOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

        int rowNum = vecs.get(0).size();
        List<Vec> nulls = new ArrayList<>();
        for (int i = 0; i < vecs.size(); i++) {
            IntVec nullVec1 = new IntVec(rowNum);
            IntVec nullVec2 = new IntVec(rowNum);
            nulls.add(nullVec1);
            nulls.add(nullVec2);

            orderBy.addInput(contextAddress, sortAddress, vecs, nulls, 10, 2);
        }

        orderBy.execute(contextAddress, sortAddress);
        OMResult result = orderBy.getOutput(contextAddress, sortAddress);
        elapsed = System.currentTimeMillis() - start;
        System.out.println("getResult elapsed time : " + elapsed + " ms");

        ByteBuffer[] output = result.getBuffers();
    }

    private List<Vec> buildVecs()
    {
        int totalPageCount = 10;
        int pageDistinctCount = 4;
        int pageDistinctValueRepeatCount = 250000;

        List<Vec> vecs = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            IntVec intVec1 = new IntVec(pageDistinctCount * pageDistinctValueRepeatCount);
            IntVec intVec2 = new IntVec(pageDistinctCount * pageDistinctValueRepeatCount);
            int idx = 0;
            for (int j = 0; j < pageDistinctCount; j++) {
                for (int k = 0; k < pageDistinctValueRepeatCount; k++) {
                    intVec1.set(idx, j);
                    intVec2.set(idx, j);
                    idx++;
                }
            }
            vecs.add(intVec1);
            vecs.add(intVec2);
        }
        return vecs;
    }
}
