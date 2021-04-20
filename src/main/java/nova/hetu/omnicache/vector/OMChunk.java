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
package nova.hetu.omnicache.vector;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class OMChunk
{
    private ByteBuffer data;
    private long memAddress;
    private AtomicInteger referenceCount;
    private boolean isReleasable = true;

    public OMChunk(ByteBuffer data)
    {
        this.data = data;
        this.referenceCount = new AtomicInteger(1);
        this.memAddress = ((DirectBuffer) data).address();
    }

    public OMChunk(OMChunk buf)
    {
        this.data = buf.data;
        this.memAddress = ((DirectBuffer) data).address();
        referenceCount = buf.referenceCount;
        referenceCount.incrementAndGet();
    }

    public ByteBuffer getData()
    {
        return data;
    }

    public long getAddress()
    {
        return this.memAddress;
    }

    public boolean release()
    {
        return isReleasable && release(-1);
    }

    public boolean release(int decrement)
    {
        if (!isReleasable) {
            return false;
        }
        if (referenceCount.addAndGet(decrement) == 0) {
            OMVectorBase.release(memAddress);
            return true;
        }
        return false;
    }

    public void setReleasable(boolean isReleasable)
    {
        this.isReleasable = isReleasable;
    }
}
