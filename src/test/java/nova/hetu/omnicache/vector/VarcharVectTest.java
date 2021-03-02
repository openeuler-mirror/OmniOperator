package nova.hetu.omnicache.vector;

import org.testng.Assert;
import org.testng.annotations.Test;

public class VarcharVectTest
{

    @Test
    public void testHappy() {
        String first = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu";
        String second = "Llanfairpwllgwyngyll";
        String third = "Lake_Chaubunagungamaug";
        byte[] element1 = first.getBytes();
        byte[] element2 = second.getBytes();
        byte[] element3 = third.getBytes();
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
    }
}
