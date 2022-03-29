
package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import org.testng.annotations.Test;

/**
 * test vec allocator
 */
public class TestVecAllocator {
    @Test
    public void testAllocatorBasic() {
        long limit = 4096;
        long subLimit = 2048;
        int size = 100;

        VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR.newChildAllocator("parent", limit, 0);
        vecAllocator.setLimit(limit);
        VecAllocator subVecAllocator1 = vecAllocator.newChildAllocator("operator", subLimit, 0);
        VecAllocator subVecAllocator2 = vecAllocator.newChildAllocator("operator", subLimit, 0);

        assertEquals(vecAllocator.getScope(), "parent");
        assertEquals(vecAllocator.getLimit(), limit);
        for (VecAllocator subVecAllocator : vecAllocator.getChildAllocators()) {
            assertEquals(subVecAllocator.getParentAllocator().getNativeAllocator(), vecAllocator.getNativeAllocator());
            assertEquals(subVecAllocator.getLimit(), subLimit);
            assertEquals(subVecAllocator.getScope(), "operator");
        }

        DecimalVec decimal128Vec = new Decimal128Vec(vecAllocator, size);
        LongVec longVec = new LongVec(subVecAllocator1, size);
        IntVec intVec = new IntVec(subVecAllocator2, size);

        assertEquals(vecAllocator.getAllocatedMemory(), (Long.BYTES * 2 + 1) * size
                + subVecAllocator1.getAllocatedMemory() + subVecAllocator2.getAllocatedMemory());
        assertEquals(subVecAllocator1.getAllocatedMemory(), (long) (Long.BYTES + 1) * size);
        assertEquals(subVecAllocator2.getAllocatedMemory(), (long) (Integer.BYTES + 1) * size);
        assertEquals(vecAllocator.getPeakAllocated(), vecAllocator.getAllocatedMemory());

        intVec.close();
        longVec.close();
        decimal128Vec.close();
        subVecAllocator1.close();
        subVecAllocator2.close();
        vecAllocator.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = "newAllocator failed")
    public void testVecAllocatorBeyondLimit() {
        long limit = 2048;
        long subLimit = 1024;
        int size = 200;

        VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR.newChildAllocator("parent", limit, 0);
        vecAllocator.setLimit(limit);
        VecAllocator subVecAllocator = vecAllocator.newChildAllocator("operator", subLimit, 0);

        LongVec longVec = new LongVec(vecAllocator, size);
        try {
            IntVec intVec = new IntVec(subVecAllocator, size);
        } catch (OmniRuntimeException e) {
            longVec.close();
            vecAllocator.close();
            subVecAllocator.close();
            throw new OmniRuntimeException(OmniErrorType.OMNI_INNER_ERROR, "newAllocator failed");
        }
    }
}
