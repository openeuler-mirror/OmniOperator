package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class OmniFilterTest
{
    @Test
    public void testUnsupportedFilter()
    {
        IntVec intVec = new IntVec(1024);
        LongVec longVec = new LongVec(1024);
        DoubleVec doubleVec = new DoubleVec(1024);

        for (int i = 0; i < 1024; i++) {
            intVec.set(i, i);
            longVec.set(i, i);
            doubleVec.set(i, i + 0.5);
        }

        OmniFilter filter = new OmniFilter();
        FilterContext context = filter.compile("test", new int[] {1, 2, 3});
        assertFalse(context.isFilterSupported());
    }

    @Test
    public void testFilter1()
    {
        int rowCount = 3000;
        IntVec intVec = new IntVec(rowCount);
        LongVec longVec = new LongVec(rowCount);
        DoubleVec doubleVec = new DoubleVec(rowCount);
        DoubleVec doubleVec1 = new DoubleVec(rowCount);
        DoubleVec doubleVec2 = new DoubleVec(rowCount);
        DoubleVec doubleVec3 = new DoubleVec(rowCount);
        LongVec longVec2 = new LongVec(rowCount);
        String expression = "$operator$LESS_THAN_OR_EQUAL(#0, 10471)";

        for (int i = 0; i < rowCount; i++) {
            intVec.set(i, i + 10000);
            longVec.set(i, i);
            doubleVec.set(i, i + 0.5);
            doubleVec1.set(i, i + 0.6);
            doubleVec2.set(i, i + 0.7);
            doubleVec3.set(i, i + 0.8);
            longVec2.set(i, i + 10000);
        }

        OmniFilter filter = new OmniFilter();
        FilterContext context = filter.compile(expression, new int[] {1, 2, 3, 3, 3, 3, 2});

        for (int i = 0; i < 10; i++) {
            long nano = System.nanoTime();
            int[] selected_rows = filter.execute(context, new Vec[] {intVec, longVec, doubleVec, doubleVec1, doubleVec2, doubleVec3, longVec2}, rowCount);
            long total = System.nanoTime() - nano;
            System.out.println("time spent: " + total);
            assertEquals(selected_rows.length, 472);
            assertEquals(selected_rows[0], 0);
            assertEquals(selected_rows[471], 471);
        }
    }

    @Test
    public void testFilter2()
    {
        int rowCount = 1024;

        DoubleVec vec0 = new DoubleVec(rowCount);
        LongVec vec1 = new LongVec(rowCount);
        DoubleVec vec2 = new DoubleVec(rowCount);
        LongVec vec3 = new LongVec(rowCount);

        for (int i = 0; i < rowCount; i++) {
            vec0.set(i, 20.0);
            vec1.set(i, i);
            vec2.set(i, 0.06);
            vec3.set(i, 8888);
        }

        OmniFilter filter = new OmniFilter();
        FilterContext context = filter.compile("AND(AND($operator$GREATER_THAN_OR_EQUAL(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND($operator$BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))", new int[] {
                3, 2, 3, 2});

        int[] selected_rows = filter.execute(context, new Vec[] {vec0, vec1, vec2, vec3}, rowCount);
        for (int i = 0; i < rowCount; i++) {
            assertEquals(selected_rows.length, rowCount);
        }
    }

    @Test
    public void testMultiThreadFilter()
            throws InterruptedException
    {
        IntVec intVec = new IntVec(1024);
        LongVec longVec = new LongVec(1024);
        DoubleVec doubleVec = new DoubleVec(1024);

        for (int i = 0; i < 1024; i++) {
            intVec.set(i, i);
            longVec.set(i, i);
            doubleVec.set(i, i + 0.5);
        }

        OmniFilter filter = new OmniFilter();
        FilterContext context = filter.compile("$operator$LESS_THAN_OR_EQUAL(#0, 10471)", new int[] {1, 2, 3});

        System.out.println("------------------------------");
        int threadcount = 4;
        CountDownLatch downLatch = new CountDownLatch(threadcount);
        for (int tidx = 0; tidx < threadcount; tidx++) {
            Thread thread = new Thread(() -> {
                int[] selected_rows = filter.execute(context, new Vec[] {intVec, longVec, doubleVec}, 1024);
                downLatch.countDown();
            });
            thread.start();
        }
        downLatch.await();
    }
}
