package nova.hetu.omniruntime.vector;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class VarcharVecTest
{
    private final String first = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu";
    private final String second = "Llanfairpwllgwyngyll";
    private final String third = "Lake_Chaubunagungamaug";
    private final byte[] element1 = first.getBytes();
    private final byte[] element2 = second.getBytes();
    private final byte[] element3 = third.getBytes();

    @Test
    public void testHappy()
    {
        byte[] data = new byte[element1.length + element2.length + element3.length];
        VarcharVec vec = new VarcharVec(data.length, 3);
        int[] offsets = {0, element1.length, element1.length + element2.length};
        int[] lengths = {element1.length, element2.length, element3.length};
        vec.setData(0, element1);
        vec.setData(element1.length, element2);
        int thirdPosition = element2.length + element1.length;
        vec.setData(thirdPosition, element3);

        vec.set(offsets, lengths);
        assertEquals(third, new String(vec.getData(2)));
        assertEquals(second, new String(vec.getData(1)));
        assertEquals(first, new String(vec.getData(0)));
        assertEquals(first, new String(vec.getDataAtOffset(0)));
    }

//    @Test
    public void TestSlice()
    {
        // initialize varchar vector
        byte[] data = new byte[element1.length + element2.length + element3.length];
        VarcharVec vec = new VarcharVec(data.length, 3);
        int[] offsets = {0, element1.length, element2.length};
        int[] lengths = {element1.length, element2.length, element3.length};
        vec.set(offsets, lengths);
        vec.setData(0, element1);
        vec.setData(element1.length, element2);
        vec.setData(element1.length + element2.length, element3);

        VarcharVec slice1 = (VarcharVec) vec.slice(0, 0);
        assertEquals(first, new String(slice1.getData(0)));
        VarcharVec slice2 = (VarcharVec) vec.slice(1, 1);
        assertEquals(second, new String(slice2.getData(0, slice2.capacity())));
        VarcharVec slice3 = (VarcharVec) vec.slice(1, 2);
        assertEquals(second + third, new String(slice3.getData(0, slice3.capacity())));
    }

//    @Test
    public void TestZeroLengthVector()
    {
        // initialize varchar vector
        VarcharVec vec = new VarcharVec(2, 2);
        int[] offsets = {0, 1};
        int[] lengths = {1, 1};
        vec.set(offsets, lengths);
        vec.setData(0, "".getBytes());
        vec.setData(1, "".getBytes());

        // test behaviours
        assertNotEquals("", new String(vec.getDataAtOffset(0)));
        assertNotEquals(null, new String(vec.getDataAtOffset(0)));
        assertEquals(new byte[1], vec.getDataAtOffset(0));
        assertEquals(new byte[1], vec.getData(0));
        VarcharVec slice = (VarcharVec) vec.slice(0, 1);
        assertEquals(new byte[1], slice.getData(0, 1));
    }
}
