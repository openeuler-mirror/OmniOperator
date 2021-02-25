/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import nova.hetu.omnicache.vector.VecType;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Demo
{
    public static class ExecutionHandler
    {
        final String neid;
        final String omniCacheKey;
        final OmniRuntime omniRuntime;
        boolean isConsole = false;

        public ExecutionHandler(OmniRuntime omniRuntime)
        {
            this.neid = omniRuntime.compile("|v0 :vec[vec[i64]], v1: vec[vec[i64]]|" +
                    "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i64,i64,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| " + "merge(b, {m.$0, m.$1})))));" +
                    "let k = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$0)));" +
                    "let v = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$1)));" +
                    "{k,v}");
            this.omniCacheKey = UUID.randomUUID().toString();
            this.omniRuntime = omniRuntime;
        }

        public void compute(int expectedValue)
        {
            Vec[] input = builderRawData();
            Vec[] res = (Vec[]) omniRuntime.execute(neid, omniCacheKey, input, 1, new VecType[] {VecType.LONG, VecType.LONG}, OmniOpStep.INTERMEDIATE);
            for (Vec v : input) {
                v.close();
            }
            LongVec interValue = (LongVec) res[1];
            LongVec interKey = (LongVec) res[0];
            if (!isConsole && (interKey.get(0) != 1 || interValue.get(0) != expectedValue)) {
                String msg = Thread.currentThread().getName() + ",Key=" + interKey.get(0) + ",value=" + interValue.get(0) + ",expected:" + expectedValue;
                System.out.println(msg);
                isConsole = true;
            }
        }

        public void getFinalResult(int expectedValue)
        {
            Vec[] res = (Vec[]) omniRuntime.getResults(omniCacheKey, new VecType[] {VecType.LONG, VecType.LONG});
            LongVec interValue = (LongVec) res[1];
            LongVec interKey = (LongVec) res[0];
            System.out.println("result:" + Thread.currentThread().getName() + ",Key=" + interKey.get(0) + ",value=" + interValue.get(0) + ",expected:" + expectedValue);
        }
    }

    private static final OmniRuntime omniRuntime = new OmniRuntime();

    public static void multiThreadExecution()
    {
        int threadCount = 100;
        int totalPageCount = 100000;
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        ExecutionHandler executionHandler = new ExecutionHandler(omniRuntime);
                        singleThreadExecution(executionHandler, totalPageCount);
                    }
                    finally {
                        downLatch.countDown();
                    }
                }
            });
            thread.setName("thread-" + tIdx);
            thread.start();
        }
        try {
            downLatch.await();
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void singleThreadExecution(ExecutionHandler executionHandler, int totalPageCount)
    {
        for (int i = 1; i <= totalPageCount; i++) {
            executionHandler.compute(i);
        }
        executionHandler.getFinalResult(totalPageCount);
    }

    public static Vec[] builderRawData()
    {
        LongVec key = new LongVec(1);
        LongVec value = new LongVec(1);
        key.set(0, 1);
        value.set(0, 1);
        Vec[] rawData = {key, value};
        return rawData;
    }

    public static UnsafeLongVec[] builderUnSafeVec()
    {
        UnsafeLongVec key = new UnsafeLongVec(1);
        UnsafeLongVec value = new UnsafeLongVec(1);
        return new UnsafeLongVec[] {key, value};
    }

    public static void main(String[] args)
            throws InterruptedException
    {
//        multiThreadExecution();
//        UnsafeLongVec longVec = new UnsafeLongVec(1);
//        longVec.set(10,10L);
//        System.out.println(longVec.get(10));
//        int threadCount = 100;
//        int pageCount = 1000000;
//        multiThreadUnsafeVecBuilderAndFree(threadCount, pageCount);
//        multiThreadVecBuilderAndFree(threadCount, pageCount);
        multiThreadExecution();
        Thread.sleep(1000000);
    }

    public static void multiThreadUnsafeVecBuilderAndFree(int threadCount, int pageCount)
    {
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        UnsafeLongVec[] input = builderUnSafeVec();
                        for (UnsafeLongVec v : input) {
                            v.release();
                        }
                    }
                }
                finally {
                    downLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            downLatch.await();
            ;
            System.out.println("[Unsafe]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
            System.gc();
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void multiThreadVecBuilderAndFree(int threadCount, int pageCount)
    {
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        Vec[] input = builderRawData();
                        for (Vec v : input) {
                            v.close();
                        }
                    }
                }
                finally {
                    downLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            downLatch.await();
            System.out.println("[Vec]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public final static void demoShow()
    {
        int[] value0 = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] value1 = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        ByteBuffer[] buffers = new ByteBuffer[2];
        IntVec v0 = new IntVec(20);
        for (int i = 0; i < value0.length; i++) {
            v0.set(i, value0[i]);
        }
        IntVec v1 = new IntVec(20);
        for (int i = 0; i < value1.length; i++) {
            v1.set(i, value1[i]);
        }
        buffers[0] = v0.getData();
        buffers[1] = v1.getData();
        VecType[] outputTypes = {VecType.INT, VecType.INT};
        int rowNum = 12;
        System.out.println("....." + v0.size());
        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| " + "merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        OmniRuntime omniRuntime = new OmniRuntime();
        String neid = omniRuntime.compile(code);
        Vec[] vecs = {v0, v1};
        Vec[] resultVecs = (Vec[]) omniRuntime.execute(neid, "TMP", vecs, rowNum, outputTypes, OmniOpStep.FINAL);

        for (int i = 0; i < resultVecs[0].size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int idx = 0; idx < resultVecs.length; idx++) {
                sb.append(((IntVec) resultVecs[idx]).get(i)).append("\t");
            }
            System.out.println(sb.toString());
        }

        int[] value0_ = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] value1_ = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        ByteBuffer[] buffers_ = new ByteBuffer[2];
        IntVec v0_ = new IntVec(20);
        for (int i = 0; i < value0_.length; i++) {
            v0_.set(i, value0[i]);
        }
        IntVec v1_ = new IntVec(20);
        for (int i = 0; i < value1_.length; i++) {
            v1_.set(i, value1[i]);
        }
        buffers_[0] = v0.getData();
        buffers_[1] = v1.getData();
        String neid_ = omniRuntime.compile(code);
        Vec[] vecs_ = {v0_, v1_};
        Vec[] resultVecs_ = (Vec[]) omniRuntime.execute(neid_, "TMP", vecs_, rowNum, outputTypes, OmniOpStep.FINAL);

        for (int i = 0; i < resultVecs_[0].size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int idx = 0; idx < resultVecs_.length; idx++) {
                sb.append(((IntVec) resultVecs_[idx]).get(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
}
