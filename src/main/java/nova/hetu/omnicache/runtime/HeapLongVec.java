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

public class HeapLongVec
{
    private final long[] values;
    public HeapLongVec(int capacity){
        values=new long[capacity];
    }
    public void set(int idx, long value)
    {
        values[idx] = value;
    }

    public long get(int idx)
    {
        return values[idx];
    }

    public void release()
    {
    }
}
