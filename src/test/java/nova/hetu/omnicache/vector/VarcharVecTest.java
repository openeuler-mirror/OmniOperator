package nova.hetu.omnicache.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

public class VarcharVecTest
{
    private final String first = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu";
    private final String second = "Llanfairpwllgwyngyll";
    private final String third = "Lake_Chaubunagungamaug";
    private final byte[] element1 = first.getBytes();
    private final byte[] element2 = second.getBytes();
    private final byte[] element3 = third.getBytes();

    @Test
    public void testHappy() {
        byte[] data = new byte[element1.length + element2.length + element3.length];
        VarcharVec vec = new VarcharVec(data.length, 3);
        int[] offsets = {0, element1.length, element1.length + element2.length};
        int[] lengths = {element1.length, element2.length, element3.length};
        vec.setData(0, element1);
        vec.setData(element1.length, element2);
        int thirdPosition = element2.length + element1.length;
        vec.setData(thirdPosition, element3);

        vec.set(offsets, lengths);
        Assert.assertEquals(third, new String(vec.getData(2)));
        Assert.assertEquals(second, new String(vec.getData(1)));
        Assert.assertEquals(first, new String(vec.getData(0)));
        Assert.assertEquals(first, new String(vec.getDataAtOffset(0)));
    }

    @Test
    public void TestSlice() {
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
        Assert.assertEquals(first, new String(slice1.getData(0)));
        VarcharVec slice2 = (VarcharVec) vec.slice(1, 1);
        Assert.assertEquals(second, new String(slice2.getData(0, slice2.capacity())));
        VarcharVec slice3 = (VarcharVec) vec.slice(1, 2);
        Assert.assertEquals(second + third, new String(slice3.getData(0, slice3.capacity())));
    }

    @Test
    public void TestZeroLengthVector() {
        // initialize varchar vector
        VarcharVec vec = new VarcharVec(2, 2);
        int[] offsets = {0, 1};
        int[] lengths = {1, 1};
        vec.set(offsets, lengths);
        vec.setData(0, "".getBytes());
        vec.setData(1, "".getBytes());

        // test behaviours
        Assert.assertNotEquals("", new String(vec.getDataAtOffset(0)));
        Assert.assertNotEquals(null, new String(vec.getDataAtOffset(0)));
        Assert.assertEquals(new byte[1], vec.getDataAtOffset(0));
        Assert.assertEquals(new byte[1], vec.getData(0));
        VarcharVec slice = (VarcharVec) vec.slice(0, 1);
        Assert.assertEquals(new byte[1], slice.getData(0, 1));
    }

}
