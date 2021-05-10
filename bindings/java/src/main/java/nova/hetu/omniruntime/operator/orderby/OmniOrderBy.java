package nova.hetu.omniruntime.operator.orderby;

import nova.hetu.omniruntime.operator.OMResult;
import nova.hetu.omniruntime.operator.OmniRuntime;
import nova.hetu.omniruntime.vector.Vec;

import java.util.List;

public class OmniOrderBy
        extends OmniRuntime
{
    public long createOperatorFactory(int[] sourceTypes, int typeCount, int[] outputCols, int outputColCount, int[] sortCols, int[] ascendings, int[] nullFirsts, int sortColCount)
    {
        long factoryAddress = getJniWrapper().createSortOperatorFactory(sourceTypes, outputCols, sortCols, ascendings, nullFirsts);
        return factoryAddress;
    }

    public long createOperator(long factoryAddress)
    {
        long operatorAddress = getJniWrapper().createSortOperator(factoryAddress);
        return operatorAddress;
    }

    public void addInput(long operatorAddress, List<Vec> datas, int pageCount, int colCount)
    {
        int vecCount = datas.size();
        long[] dataAddrs = new long[vecCount];
        int[] positionCounts = new int[pageCount];  // positionCount for every page

        int idx = 0;
        for (int i = 0; i < vecCount; i++) {
            Vec dataVec = datas.get(i);
            dataAddrs[i] = dataVec.getAddress();

            if (i % colCount == 0) {
                int rowNum = dataVec.size();
                positionCounts[idx++] = rowNum;
            }
        }

        getJniWrapper().addSortInput(operatorAddress, dataAddrs, positionCounts, pageCount);
    }

    public OMResult getOutput(long operatorAddress)
    {
        OMResult[] result = getJniWrapper().getSortOutput(operatorAddress);
        return result[0];
    }
}
