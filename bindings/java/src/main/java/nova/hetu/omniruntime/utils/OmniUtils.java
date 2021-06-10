package nova.hetu.omniruntime.utils;

import nova.hetu.omniruntime.vector.AggType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecType;

import java.util.List;

import static java.lang.String.format;

public class OmniUtils
{
    private OmniUtils()
    {
    }

    public static LongVec transformVecAddress(List<Vec> inputs)
    {
        LongVec address = new LongVec(inputs.size());
        for (int idx = 0; idx < inputs.size(); idx++) {
            address.set(idx, inputs.get(idx).getAddress());
        }
        return address;
    }

    public static int[] transformVecType(VecType[] vecTypes)
    {
        int[] vecTypeValue = new int[vecTypes.length];
        for (int idx = 0; idx < vecTypes.length; idx++) {
            vecTypeValue[idx] = vecTypes[idx].getValue();
        }
        return vecTypeValue;
    }

    public static IntVec getRowNumbers(List<Vec> inputs, int columnCount)
    {
        int totalColumn = inputs.size();
        if (totalColumn % columnCount != 0) {
            throw new IllegalArgumentException(format("input vec error:total colum: %s,column count: %s", totalColumn, columnCount));
        }

        int pageNum = totalColumn / columnCount;
        IntVec rowNums = new IntVec(pageNum);
        for (int idx = 0; idx < pageNum; idx++) {
            rowNums.set(idx, inputs.get(idx * columnCount).size());
        }
        return rowNums;
    }

    public static int[] transformAggType(AggType[] aggTypes)
    {
        int[] aggTypeValue = new int[aggTypes.length];
        for (int idx = 0; idx < aggTypes.length; idx++) {
            aggTypeValue[idx] = aggTypes[idx].getValue();
        }
        return aggTypeValue;
    }
}
