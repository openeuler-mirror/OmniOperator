package nova.hetu.omniruntime.vector;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * merge vector test
 */
public class MergeVectorsTest {
    /**
     * test int vector merge
     */
    @Test
    public void testIntVectorsMerge() {
        IntVec vec1 = new IntVec(10000);
        IntVec vec2 = new IntVec(20000);
        IntVec vec3 = new IntVec(40000);
        IntVec vec = new IntVec(70000);
        for (int i = 0; i < vec1.getSize(); i++) {
            vec1.set(i, i);
        }
        for (int i = vec1.getSize(), j = 0; i < vec2.getSize() + vec1.getSize(); i++, j++) {
            vec2.set(j, i);
        }
        for (int i = vec1.getSize() + vec2.getSize(), j = 0; i < vec3.getSize() + vec2.getSize() + vec1.getSize(); i++, j++) {
            vec3.set(j, i);
        }
        vec.append(vec1, 0, vec1.getSize());
        vec.append(vec2, vec1.getSize(), vec2.getSize());
        vec.append(vec3, vec1.getSize() + vec2.getSize(), vec3.getSize());
        for (int i = 0; i < vec1.getSize() + vec2.getSize() + vec3.getSize(); i++) {
            assertEquals(vec.get(i), i);
        }
    }

    /**
     * test double vector merge
     */
    @Test
    public void testDoubleVectorsMerge() {
        DoubleVec vec1 = new DoubleVec(10000);
        DoubleVec vec2 = new DoubleVec(20000);
        DoubleVec vec3 = new DoubleVec(40000);
        DoubleVec vec = new DoubleVec(70000);
        // Creating and appending Vector 1
        for (int i = 0; i < vec1.getSize(); i++) {
            vec1.set(i, (double) i);
        }
        vec.append(vec1, 0, vec1.getSize());
        // Creating and appending Vector 2
        for (int i = vec1.getSize(), j = 0; i < vec2.getSize() + vec1.getSize(); i++, j++) {
            vec2.set(j, (double) i);
        }
        vec.append(vec2, vec1.getSize(), vec2.getSize());
        // Creating and appending Vector 3
        for (int i = vec1.getSize() + vec2.getSize(), j = 0; i < vec3.getSize() + vec2.getSize() + vec1.getSize(); i++, j++) {
            vec3.set(j, (double) i);
        }
        vec.append(vec3, vec1.getSize() + vec2.getSize(), vec3.getSize());

        for (int i = 0; i < vec1.getSize() + vec2.getSize() + vec3.getSize(); i++) {
            assertEquals(vec.get(i), (double) i);
        }
    }

    /**
     * test long vector merge
     */
    @Test
    public void testLongVectorsMerge() {
        LongVec vec1 = new LongVec(10000);
        LongVec vec2 = new LongVec(20000);
        LongVec vec3 = new LongVec(40000);
        LongVec vec = new LongVec(70000);
        // Creating and appending Vector 1
        for (int i = 0; i < vec1.getSize(); i++) {
            vec1.set(i, (long) i);
        }
        vec.append(vec1, 0, vec1.getSize());
        // Creating and appending Vector 2
        for (int i = vec1.getSize(), j = 0; i < vec2.getSize() + vec1.getSize(); i++, j++) {
            vec2.set(j, (long) i);
        }
        vec.append(vec2, vec1.getSize(), vec2.getSize());
        // Creating and appending Vector 3
        for (int i = vec1.getSize() + vec2.getSize(), j = 0; i < vec3.getSize() + vec2.getSize() + vec1.getSize(); i++, j++) {
            vec3.set(j, (long) i);
        }
        vec.append(vec3, vec1.getSize() + vec2.getSize(), vec3.getSize());

        for (int i = 0; i < vec1.getSize() + vec2.getSize() + vec3.getSize(); i++) {
            assertEquals(vec.get(i), (long) i);
        }
    }
}
