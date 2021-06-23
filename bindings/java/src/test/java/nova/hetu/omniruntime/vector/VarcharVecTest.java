package nova.hetu.omniruntime.vector;

import static org.testng.Assert.assertEquals;

public class VarcharVecTest
{
    private final String first = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu";
    private final String second = "Llanfairpwllgwyngyll";
    private final String third = "Lake_Chaubunagungamaug";
    private final byte[] element1 = first.getBytes();
    private final byte[] element2 = second.getBytes();
    private final byte[] element3 = third.getBytes();

    //    @Test
    public void testHappy()
    {
        byte[] data = new byte[element1.length + element2.length + element3.length];
        VarcharVec vec = new VarcharVec(data.length, 3);
        int offset = element1.length;
        vec.setValue(0, element1);
        vec.setValueOffset(0, offset);
        vec.setValue(1, element2);
        offset += element2.length;
        vec.setValueOffset(1, offset);
        vec.setValue(2, element3);
        offset += element3.length;
        vec.setValueOffset(2, offset);

        assertEquals(third, new String(vec.getValue(2)));
        assertEquals(second, new String(vec.getValue(1)));
        assertEquals(first, new String(vec.getValue(0)));
    }

    //    @Test
    public void TestSlice()
    {
        // initialize varchar vector
        byte[] data = new byte[element1.length + element2.length + element3.length];
        VarcharVec vec = new VarcharVec(data.length, 3);
        int offset = element1.length;
        vec.setValue(0, element1);
        vec.setValueOffset(0, offset);
        vec.setValue(1, element2);
        offset += element2.length;
        vec.setValueOffset(1, offset);
        vec.setValue(2, element3);
        offset += element3.length;
        vec.setValueOffset(2, offset);

        VarcharVec slice1 = (VarcharVec) vec.slice(0, 0);
        assertEquals(first, new String(slice1.getValue(0)));
        VarcharVec slice2 = (VarcharVec) vec.slice(1, 1);
        assertEquals(second, new String(slice2.getValue(0)));
        VarcharVec slice3 = (VarcharVec) vec.slice(1, 2);
        assertEquals(third, new String(slice3.getValue(0)));
    }

    //    @Test
    public void TestZeroLengthVector()
    {
        // initialize varchar vector
        VarcharVec vec = new VarcharVec(2, 2);
        vec.setValue(0, "".getBytes());
        vec.setValueOffset(0, 0);
        vec.setValue(1, "".getBytes());
        vec.setValueOffset(1, 0);

        // test behaviours
        assertEquals("", new String(vec.getValue(0)));
        VarcharVec slice = (VarcharVec) vec.slice(0, 1);
        assertEquals("", slice.getValue(0));
    }
}
