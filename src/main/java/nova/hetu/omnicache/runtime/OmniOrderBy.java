package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.Vec;

import java.util.List;

public class OmniOrderBy
        extends OmniRuntime
{
    public long prepare(int[] sourceTypes, int typeCount, int[] outputCols, int outputColCount, int[] sortCols, int[] ascendings, int[] nullFirsts, int sortColCount)
    {
        long contextAddress = getJniWrapper().sortPrepare(sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
        return contextAddress;
    }

    public long createOperator(long contextAddress, int[] sourceTypes, int typeCount, int[] outputCols, int outputColCount, int[] sortCols, int[] ascendings, int[] nullFirsts, int sortColCount)
    {
        long sortAddress = getJniWrapper().sortCreateOperator(contextAddress, sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
        return sortAddress;
    }

    public void addInput(long contextAddress, long sortAddress, List<Vec> datas, List<Vec> nulls, int pageCount, int colCount)
    {
        int vecCount = datas.size();
        long[] dataAddrs = new long[vecCount];
        long[] nullAddrs = new long[vecCount];
        long[] positionCounts = new long[pageCount];  // positionCount for every page
        long totalRowNum = 0;

        int idx = 0;
        for (int i = 0; i < vecCount; i++) {
            Vec dataVec = datas.get(i);
            dataAddrs[i] = dataVec.getAddress();
            nullAddrs[i] = nulls.get(i).getAddress();

            if (i % colCount == 0) {
                int rowNum = dataVec.size();
                positionCounts[idx++] = rowNum;
                totalRowNum += rowNum;
            }
        }

        getJniWrapper().sortAddInput(contextAddress, sortAddress, dataAddrs, nullAddrs, pageCount, positionCounts, totalRowNum);
    }

    public void execute(long contextAddress, long sortAddress)
    {
        getJniWrapper().sortExecute(contextAddress, sortAddress);
    }

    public OMResult getOutput(long contextAddress, long sortAddress)
    {
        OMResult result = getJniWrapper().sortGetOutput(contextAddress, sortAddress);
        return result;
    }
}
