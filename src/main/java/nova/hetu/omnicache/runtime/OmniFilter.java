/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;

import java.util.ArrayList;
import java.util.List;

public class OmniFilter
{
    JniWrapper jniWrapper = new JniWrapper();

    public FilterContext compile(String filterExpr, int[] columnTypes)
    {
        FilterContext filterContext = new FilterContext();
        filterContext.setInputVecTypes(columnTypes);

        long filterAddr = jniWrapper.filterCompile(filterExpr, filterContext.getInputVecTypes().getAddress(), columnTypes.length);

        if (filterAddr == 0) {
            filterContext.setFilterSupported(false);
            return filterContext;
        }

        filterContext.setFilterSupported(true);
        filterContext.setFilterId(filterAddr);
        return filterContext;
    }

    public int[] execute(FilterContext filterContext, Vec[] inputs, int rowCount)
    {
        long[] inputRawAddressArr = transformVecToAddresses(inputs);
        IntVec selectedPosition = new IntVec(rowCount);

        int selectCount = jniWrapper.filterExecute(filterContext.getFilterId(), inputRawAddressArr, filterContext.getInputVecTypes().getAddress(), inputs.length, selectedPosition.getAddress(), rowCount);

        int[] positions = new int[selectCount];
        selectedPosition.getData().asIntBuffer().get(positions);
        selectedPosition.close();

        return positions;
    }

    public OMFilterResult executeV1(FilterContext filterContext, Vec[] inputs, int rowCount, int[] projectIdx)
    {
        LongVec[] projects = new LongVec[projectIdx.length];
        for (int i = 0; i < projectIdx.length; i++) {
            projects[i] = new LongVec(rowCount);
        }
        long[] inputRawAddressArr = transformVecToAddresses(inputs);
        long[] projectAddressArr = transformVecToAddresses(projects);

        int selectedCount = jniWrapper.filterExecuteV1(filterContext.getFilterId(), inputRawAddressArr, filterContext.getInputVecTypes().getAddress(), inputs.length, rowCount, projectAddressArr, projectIdx, projectIdx.length);

        if (selectedCount == rowCount) {
            for (int i = 0; i < projects.length; i++) {
                projects[i].close();
            }
            List<Vec> result = new ArrayList<>();
            for (int i = 0; i < projectIdx.length; i++) {
                result.add(inputs[projectIdx[i]]);
            }
            return new OMFilterResult(result.toArray(new Vec[0]), rowCount, true);
        }
        else {
            for (int i = 0; i < projects.length; i++) {
                projects[i].setSize(selectedCount);
            }
            return new OMFilterResult(projects, selectedCount, false);
        }
    }

    public void finished(FilterContext filterContext)
    {
        //TODO:Do Release filter module off-heap resource
        jniWrapper.filterFinished(filterContext.getFilterId());
        filterContext.finished();
    }

    private long[] transformVecToAddresses(Vec[] inputs)
    {
        if (inputs != null) {
            long[] addresses = new long[inputs.length];
            for (int idx = 0; idx < inputs.length; idx++) {
                addresses[idx] = inputs[idx].getAddress();
            }
            return addresses;
        }
        return null;
    }
}
