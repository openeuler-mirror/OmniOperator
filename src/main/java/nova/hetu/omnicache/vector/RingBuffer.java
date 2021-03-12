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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class RingBuffer
{
    private ByteBuffer[] bufferPool;
    public AtomicInteger head = new AtomicInteger(0);
    public AtomicInteger tail = new AtomicInteger(0);
    private int bufferSize;
    private Object readLock = new Object();
    private Object writeLock = new Object();
    public AtomicInteger REUSED = new AtomicInteger(0);

    public RingBuffer(int initSize)
    {
        this.bufferSize = initSize;

        this.bufferPool = new ByteBuffer[bufferSize];
    }

    private Boolean empty()
    {
        return head.get() == tail.get();
    }

    private Boolean full()
    {
        return (tail.get() + 1) % bufferSize == head.get();
    }

    public void clear()
    {
        Arrays.fill(bufferPool, null);
        this.head.set(0);
        this.tail.set(0);
    }

    public Boolean add(ByteBuffer buffer)
    {
        if (full()) {
            return false;
        }
        ((Buffer) buffer).clear();
        bufferPool[tail.incrementAndGet() % bufferSize] = buffer;
        synchronized (writeLock) {
            tail.set(tail.get() % bufferSize);
        }
        return true;
    }

    public ByteBuffer poll()
    {
        if (empty()) {
            return null;
        }
        ByteBuffer result = bufferPool[head.incrementAndGet() % bufferSize];
        REUSED.incrementAndGet();
        synchronized (readLock) {
            head.set(head.get() % bufferSize);
        }
        return result;
    }

    public ByteBuffer[] getAll()
    {
//            if (empty()) {
//                return null;
//            }
//            int copyTail = tail;
//            int cnt = head < copyTail ? copyTail - head : bufferSize - head + copyTail;
//            ByteBuffer[] result = new ByteBuffer[cnt];
//            if (head < copyTail) {
//                for (int i = head; i < copyTail; i++) {
//                    result[i - head] = buffer[i];
//                }
//            } else {
//                for (int i = head; i < bufferSize; i++) {
//                    result[i - head] = buffer[i];
//                }
//                for (int i = 0; i < copyTail; i++) {
//                    result[bufferSize - head + i] = buffer[i];
//                }
//            }
//            head = copyTail;
//            return result;
        return null;
    }

    public int size()
    {
        return Math.abs(tail.get() - head.get());
    }
}