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
        final OmniCodeGen omniCodeGen;
        boolean isConsole = false;

        public ExecutionHandler(OmniCodeGen omniCodeGen)
        {
            this.neid = omniCodeGen.compile("|v0 :vec[vec[i64]], v1: vec[vec[i64]]|" +
                    "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i64,i64,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| " + "merge(b, {m.$0, m.$1})))));" +
                    "let k = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$0)));" +
                    "let v = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$1)));" +
                    "{k,v}");
            this.omniCacheKey = UUID.randomUUID().toString();
            this.omniCodeGen = omniCodeGen;
        }
        public Vec[] builderRawData()
        {
            LongVec key = new LongVec(1);
            LongVec value = new LongVec(1);
            key.set(0,1);
            value.set(0,1);
            Vec[] rawData = {key, value};
            return rawData;
        }
        public void compute(int expectedValue)
        {
            Vec[] input = builderRawData();
            Vec[] res = (Vec[]) omniCodeGen.execute(neid, omniCacheKey, input, 1, new VecType[] {VecType.LONG, VecType.LONG}, OmniOpStep.INTERMEDIATE);
//            for (Vec v : input) {
//                v.close();
//            }
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
            Vec[] res = (Vec[]) omniCodeGen.getResults(omniCacheKey, new VecType[] {VecType.LONG, VecType.LONG});
            LongVec interValue = (LongVec) res[1];
            LongVec interKey = (LongVec) res[0];
            if(interKey.get(0) != 1 || interValue.get(0)!=expectedValue){
                System.out.println("Invalid result:" + Thread.currentThread().getName() + ",Key=" + interKey.get(0) + ",value=" + interValue.get(0) + ",expected:" + expectedValue);
            }else {
                System.out.println("result:" + Thread.currentThread().getName() + ",Key=" + interKey.get(0) + ",value=" + interValue.get(0) + ",expected:" + expectedValue);
            }
        }
    }


    private static final OmniCodeGen OMNI_CODE_GEN = new OmniCodeGen();

    public static void multiThreadExecution()
    {
        int threadCount = 10000;
        int totalPageCount = 1000;
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        ExecutionHandler executionHandler = new ExecutionHandler(OMNI_CODE_GEN);
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

    public static Vec[] builderRawData(int pageSize)
    {
        LongVec key = new LongVec(pageSize);
        LongVec value = new LongVec(pageSize);
        for (int idx = 0; idx < pageSize; idx++) {
            key.set(idx, idx);
            value.set(idx, idx);
        }
        Vec[] rawData = {key, value};
        return rawData;
    }


    public static ByteBufferLongVec[] buildByteBufferRawData(int pageSize)
    {
        ByteBufferLongVec key = new ByteBufferLongVec(pageSize);
        ByteBufferLongVec value = new ByteBufferLongVec(pageSize);
        for (int idx = 0; idx < pageSize; idx++) {
            key.set(idx, idx);
            value.set(idx, idx);
        }
        ByteBufferLongVec[] rawData = {key, value};

        return rawData;
    }

    public static UnsafeLongVec[] builderUnSafeVec(int pageSize)
    {
        UnsafeLongVec key = new UnsafeLongVec(pageSize);
        UnsafeLongVec value = new UnsafeLongVec(pageSize);
        for (int idx = 0; idx < pageSize; idx++) {
            key.set(idx, idx);
            value.set(idx, idx);
        }
        return new UnsafeLongVec[] {key, value};
    }
    public static HeapLongVec[] builderHeapVec(int pageSize){
        HeapLongVec key = new HeapLongVec(pageSize);
        HeapLongVec value = new HeapLongVec(pageSize);
        for (int idx = 0; idx < pageSize; idx++) {
            key.set(idx, idx);
            value.set(idx, idx);
        }
        return new HeapLongVec[] {key, value};
    }

    public static String getValue(String[] args, int idx, String expected)
    {
        if (args == null || args.length <= idx) {
            return expected;
        }
        else {
            return args[idx];
        }
    }
    public static interface LoopOperator{
        public void execute();
    }
    private static void loop(LoopOperator handle,int loopCount){
        for(int idx = 0 ;idx<loopCount;idx++){
            handle.execute();
        }
    }
    public static class R4 implements AutoCloseable{

        @Override
        public void close()
                throws Exception
        {
            System.out.println("Auto close call");
        }
    }
    public static void main(String[] args)
            throws InterruptedException
    {
        String tag = "2";
        if (args != null && args.length > 0) {
            tag = args[0];
        }
        switch (tag) {
            case "1": //data valid
                boolean close = Boolean.parseBoolean(getValue(args, 1, "false"));
                Vec.isClose.set(close);
                multiThreadExecution();
                break;
            case "2"://vec performance
                int threadCount = Integer.parseInt(getValue(args, 1, "100"));
                int pageCount = Integer.parseInt(getValue(args, 2, "1000"));
                int pageSize = Integer.parseInt(getValue(args, 3, "1000"));
                int loopCount = Integer.parseInt(getValue(args,4,"10"));
                final boolean isClose = Boolean.parseBoolean(getValue(args, 5, "false"));
                Vec.isClose.set(isClose);
                String method =getValue(args,6,"default").toLowerCase();
                switch (method){
                    case "heapvec":
                        loop(()->multiThreadHeapVecBuilderAndFree(threadCount,pageCount,pageSize,isClose),loopCount);
                        break;
                    case "unsafevec":
                        loop(()->multiThreadUnsafeVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        break;
                    case "omnivec":
                        loop(()->multiThreadVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        break;
                    case "bytebuffervec":
                        loop(()->multiThreadByteBufferVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        break;
                    default:
                        loop(()->multiThreadHeapVecBuilderAndFree(threadCount,pageCount,pageSize,isClose),loopCount);
                        loop(()->multiThreadUnsafeVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        loop(()->multiThreadVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        loop(()->multiThreadByteBufferVecBuilderAndFree(threadCount, pageCount, pageSize, isClose),loopCount);
                        break;
                }
                break;
            case "3":
                break;
            default:
                System.out.println("Not Value Tag ...");
        }
//        multiThreadExecution();
//        UnsafeLongVec longVec = new UnsafeLongVec(1);
//        longVec.set(10,10L);
//        System.out.println(longVec.get(10));
//        int threadCount = 100;
//        int pageCount = 1000000;
//        multiThreadUnsafeVecBuilderAndFree(threadCount, pageCount);
//        for (Map.Entry<Long, Integer> entry : UnsafeLongVec.addressReUsed.entrySet()) {
//            if (entry.getValue() > 1) {
//                System.out.println("key:" + entry.getKey() + ",value=" + entry.getValue());
//            }
//        }
//        multiThreadVecBuilderAndFree(threadCount, pageCount);
//        multiThreadExecution();
//        Thread.sleep(1000000);
    }

    public static void multiThreadHeapVecBuilderAndFree(int threadCount, int pageCount, int pageSize, boolean isClose){
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        HeapLongVec[] input = builderHeapVec(pageSize);
                        if (isClose) {
                            for (HeapLongVec v : input) {
                                v.release();
                            }
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
            System.out.println("[HeapVec:threadCount=" + threadCount + ",pageCount="+pageCount+",pageSize="+pageSize+",isClose="+isClose+"]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
            System.gc();
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
    public static void multiThreadUnsafeVecBuilderAndFree(int threadCount, int pageCount, int pageSize, boolean isClose)
    {
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        UnsafeLongVec[] input = builderUnSafeVec(pageSize);
                        if (isClose) {
                            for (UnsafeLongVec v : input) {
                                v.release();
                            }
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
            System.out.println("[UnsafeVec:threadCount=" + threadCount + ",pageCount="+pageCount+",pageSize="+pageSize+",isClose="+isClose+"]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
            System.gc();
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void multiThreadByteBufferVecBuilderAndFree(int threadCount, int pageCount, int pageSize, boolean isClose)
    {
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        ByteBufferLongVec[] input = buildByteBufferRawData(pageSize);
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
            System.out.println("[ByteBufferVec:threadCount=" + threadCount + ",pageCount="+pageCount+",pageSize="+pageSize+",isClose="+isClose+"]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
        }
        catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void multiThreadVecBuilderAndFree(int threadCount, int pageCount, int pageSize, boolean isClose)
    {
        long startTime = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < pageCount; i++) {
                        Vec[] input = builderRawData(pageSize);
                        if (isClose) {
                            for (Vec v : input) {
                                v.close();
                            }
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
            System.out.println("[Vec:tthreadCount=" + threadCount + ",pageCount="+pageCount+",pageSize="+pageSize+",isClose="+isClose+"]All task finished! total used time:" + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
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
        OmniCodeGen omniCodeGen = new OmniCodeGen();
        String neid = omniCodeGen.compile(code);
        Vec[] vecs = {v0, v1};
        Vec[] resultVecs = (Vec[]) omniCodeGen.execute(neid, "TMP", vecs, rowNum, outputTypes, OmniOpStep.FINAL);

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
        String neid_ = omniCodeGen.compile(code);
        Vec[] vecs_ = {v0_, v1_};
        Vec[] resultVecs_ = (Vec[]) omniCodeGen.execute(neid_, "TMP", vecs_, rowNum, outputTypes, OmniOpStep.FINAL);

        for (int i = 0; i < resultVecs_[0].size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int idx = 0; idx < resultVecs_.length; idx++) {
                sb.append(((IntVec) resultVecs_[idx]).get(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
}
