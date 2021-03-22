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

public class OmniFilter
{
    JniWrapper jniWrapper = new JniWrapper();

    public FilterContext compile(String filterExpr, int[] columnTypes)
    {
        FilterContext filterContext = new FilterContext();
        filterContext.setInputVecTypes(columnTypes);

        String filterId = jniWrapper.filterCompile(filterExpr, filterContext.getInputVecTypes().getAddress(), columnTypes.length);
        filterContext.setFilterId(filterId);
        return filterContext;
    }

    public IntVec execute(FilterContext filterContext, Vec[] inputs, int rowCount)
    {
        LongVec inputRawAddressArr = transformVecToVecArray(inputs);
        IntVec selectedPosition = new IntVec(rowCount);

        jniWrapper.filterExecute(filterContext.getFilterId(), inputRawAddressArr.getAddress(), filterContext.getInputVecTypes().getAddress(), inputRawAddressArr.size(), selectedPosition.getAddress(), rowCount);

        //release input Raw Address Array Vector
        inputRawAddressArr.close();
        return selectedPosition;
    }

    public void finished(FilterContext filterContext)
    {
        //TODO:Do Release filter module off-heap resource
        jniWrapper.filterFinished(filterContext.getFilterId());
        filterContext.finished();
    }

    private LongVec transformVecToVecArray(Vec[] inputs)
    {
        if (inputs != null) {
            LongVec inputVecAddrs = new LongVec(inputs.length);
            for (int idx = 0; idx < inputs.length; idx++) {
                inputVecAddrs.set(idx, inputs[idx].getAddress());
            }
            return inputVecAddrs;
        }
        return null;
    }
}
