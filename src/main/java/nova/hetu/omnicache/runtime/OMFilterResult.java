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

import nova.hetu.omnicache.vector.Vec;

public class OMFilterResult
{
    private Vec[] projects;
    private int size;
    private boolean reuse;

    public OMFilterResult(Vec[] projects, int size,boolean reuse)
    {
        this.projects = projects;
        this.size = size;
        this.reuse = reuse;
    }

    public Vec[] getProjects()
    {
        return projects;
    }

    public int getSize()
    {
        return size;
    }

    public boolean isReuse()
    {
        return reuse;
    }
}
