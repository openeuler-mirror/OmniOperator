package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.Vec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
        Vec[] datas = {vec};
        IntVec nullVec = new IntVec(8);
        Vec[] nulls = {nullVec};

        int[] sourceTypes = {1};
        int[] outputCols = {0};
        int[] sortCols = {0};
        int[] ascendings = {1};
        int[] nullFirsts = {0};
        long stageId = 0;

        long sortAddress = orderBy.allocAndInitSort(stageId, sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        orderBy.addTable(sortAddress, datas, nulls);
        orderBy.sort(sortAddress, stageId);
        OMResult result = orderBy.getResult(sortAddress, stageId);

        ByteBuffer[] output = result.getBuffers();
        int len = result.getLength();
        int[] actual = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual[i] = output[0].getInt(i * Integer.BYTES);
        }
        int[] expectd = {1, 2, 3, 4, 5, 6, 7, 8};
        Assert.assertEquals(actual, expectd);
    }

    @Test
    public void testOrderByPerformance()
    {
        long start = System.currentTimeMillis();
        Vec[][] vecs = buildVecs();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("buildVecs elapsed time : " + elapsed + " ms");

        int[] sourceTypes = {1, 1};
        int[] outputCols = {0, 1};
        int[] sortCols = {0, 1};
        int[] ascendings = {1, 1};
        int[] nullFirsts = {0, 0};
        long stageId = 1;

        start = System.currentTimeMillis();
        long sortAddress = orderBy.allocAndInitSort(stageId, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

        int rowNum = vecs[0][0].size();
        for (int i = 0; i < vecs.length; i++) {
            IntVec nullVec1 = new IntVec(rowNum);
            IntVec nullVec2 = new IntVec(rowNum);
            Vec[] nulls = {nullVec1, nullVec2};

            orderBy.addTable(sortAddress, vecs[i], nulls);
        }

        orderBy.sort(sortAddress, stageId);

        OMResult result = orderBy.getResult(sortAddress, stageId);
        elapsed = System.currentTimeMillis() - start;
        System.out.println("getResult elapsed time : " + elapsed + " ms");

        ByteBuffer[] output = result.getBuffers();

    }

    private Vec[][] buildVecs()
    {
        int totalPageCount = 10;
        int pageDistinctCount = 4;
        int pageDistinctValueRepeatCount = 2500000;

        Vec[][] vecs = new Vec[totalPageCount][2];
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
            vecs[i][0] = intVec1;
            vecs[i][1] = intVec2;
        }
        return vecs;
    }
}
