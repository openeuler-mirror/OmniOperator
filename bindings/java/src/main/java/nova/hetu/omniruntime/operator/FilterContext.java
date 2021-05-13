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
package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.vector.IntVec;

public class FilterContext
{
    private long nativeOperatorAddress;
    private int[] inputVecTypes;
    private boolean isFilterSupported;

    public long getFilterId()
    {
        return nativeOperatorAddress;
    }

    public void setFilterId(long filterModuleHandle)
    {
        this.nativeOperatorAddress = filterModuleHandle;
    }

    public int[] getInputVecTypes()
    {
        return inputVecTypes;
    }

    public void setInputVecTypes(int[] inputVecTypes)
    {
        this.inputVecTypes = inputVecTypes;
    }

    public boolean isFilterSupported()
    {
        return isFilterSupported;
    }

    public void setFilterSupported(boolean isFilterSupported)
    {
        this.isFilterSupported = isFilterSupported;
    }

    public void finished() {}
}
