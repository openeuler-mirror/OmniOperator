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

public class FilterContext
{
    private String filterId;
    private IntVec inputVecTypes;

    public String getFilterId()
    {
        return filterId;
    }

    public void setFilterId(String filterId)
    {
        this.filterId = filterId;
    }

    public IntVec getInputVecTypes()
    {
        return inputVecTypes;
    }

    public void setInputVecTypes(int[] columnTypes)
    {
        this.inputVecTypes = new IntVec(columnTypes.length);
        inputVecTypes.getData().asIntBuffer().put(columnTypes);
    }
    public void finished(){
        this.getInputVecTypes().close();
    }
}
