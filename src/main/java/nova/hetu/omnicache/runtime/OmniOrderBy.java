package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.Vec;

public class OmniOrderBy
        extends OmniRuntime
{
    public long allocAndInitSort(int[] sourceTypes, int typeCount, int[] outputCols, int outputColCount, int[] sortCols, int[] ascendings, int[] nullFirsts, int sortColCount)
    {
        long sortAddress = getJniWrapper().allocAndInitSort(sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
        return sortAddress;
    }

    public void addTable(long sortAddress, Vec[] datas, Vec[] nulls)
    {
        int colCount = datas.length;
        int rowCount = datas[0].size();
        long[] dataAddrs = new long[colCount];
        long[] nullAddrs = new long[colCount];

        for (int i = 0; i < colCount; i++) {
            dataAddrs[i] = datas[i].getAddress();
            nullAddrs[i] = nulls[i].getAddress();
        }

        getJniWrapper().addTable(sortAddress, dataAddrs, nullAddrs, rowCount);
    }

    public void sort(long sortAddress)
    {
        getJniWrapper().sort(sortAddress);
    }

    public OMResult getResult(long sortAddress)
    {
        OMResult result = getJniWrapper().getResult(sortAddress);
        return result;
    }
}
