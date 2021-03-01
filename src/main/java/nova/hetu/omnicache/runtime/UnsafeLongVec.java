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

import sun.misc.Unsafe;

import java.util.concurrent.ConcurrentHashMap;

//Java_nova_hetu_omnicache_runtime_demo_UnsafeVec_toRust
public class UnsafeLongVec
{
    public static ConcurrentHashMap<Long,Integer> addressReUsed = new ConcurrentHashMap<>();
    private final long address;
    private final int capacity;

    public UnsafeLongVec(int capacity)
    {
        this(JvmUtils.unsafe.allocateMemory(Long.BYTES * capacity),capacity);
    }
    public UnsafeLongVec(long address , int capacity){

        addressReUsed.put(address,addressReUsed.getOrDefault(address,0)+1);
        this.address = address;
        this.capacity = capacity;
    }

    public void set(int idx, long value)
    {
        assert idx < capacity;
        JvmUtils.unsafe.putLong(address + idx * Long.BYTES, value);
    }

    public long get(int idx)
    {
        assert (idx < capacity);
        return JvmUtils.unsafe.getLong(address + idx * Long.BYTES);
    }

    public void release()
    {
        JvmUtils.unsafe.freeMemory(address);
    }
    public long getAddress(){
        return address;
    }
}
