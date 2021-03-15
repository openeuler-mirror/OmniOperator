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

import nova.hetu.omnicache.vector.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

public class JniWrapperTest
{

    JniWrapper wrapper;

    @BeforeClass
    public void setUp() {
        wrapper = new JniWrapper();
    }

    @Test
    public void testOrderByOneColumn()
    {
        int[] sourceTypes = {0};
        int[] outputCols = {0};
        int[] sortCols = {0};
        int[] ascendings = {1};
        int[] nullFirsts = {0};

        long sortAddress = wrapper.allocAndInitSort(sourceTypes, 1, outputCols, 1, sortCols, ascendings, nullFirsts, 1);
        ByteBuffer[] inputs = new ByteBuffer[1];
        IntVec vec = new IntVec(8);
        vec.set(0, 5);
        vec.set(1, 3);
        vec.set(2, 2);
        vec.set(3, 6);
        vec.set(4, 1);
        vec.set(5, 4);
        vec.set(6, 7);
        vec.set(7, 8);
        inputs[0] = vec.getData();
        int[][] nulls = {{0, 0, 0, 0, 0, 0, 0, 0}};

        wrapper.addTable(sortAddress, inputs, nulls, 1, 8);
        wrapper.sort(sortAddress);
        OMResult result = wrapper.getResult(sortAddress);

        ByteBuffer[] output = result.getBuffers();
        int len = result.getLength();
        int[] actual = new int[len];
        output[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            actual[i] = output[0].getInt(i * Integer.BYTES);
        }
        int[] expectd = {1, 2, 3, 4, 5, 6, 7, 8};
        Assert.assertEquals(actual, expectd);
    }

    //@Test
    public void testUseLenFunction() {
        ByteBuffer[] buffers = new ByteBuffer[1];
        IntVec veclen = new IntVec(10);
        veclen.set(0, 2);
        veclen.set(1, 3);
        veclen.set(2, 4);
        veclen.set(3, 2);
        veclen.set(4, 1);
        buffers[0] = veclen.getData();

        int[] types = {1};

        int rowNum = 5;
        String code = "|x:vec[i32]| " +
                "let l=len(x);" +
                "let res = appender[i64];" +
                "result(merge(res,l))";
        String moduleId = wrapper.compile(code);
        // current not support the result
        OMResult res = wrapper.execute(moduleId, UUID.randomUUID().toString(), buffers, types, rowNum, types);
        Assert.assertEquals(1,res.getLength());
    }

    @Test
    public void testATwoColumn1() {
        JniWrapper jniWrapper = new JniWrapper();
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
        int[] types = {1, 1};
        int[] outputTypes = {1, 1};
        int rowNum = 12;

        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        String moduleId = jniWrapper.compile(code);
        System.out.println("moduleId: " + moduleId);
        String key = UUID.randomUUID().toString();
        OMResult omResults = jniWrapper.execute(moduleId, key, buffers, types, rowNum, outputTypes);
        ByteBuffer[] results = omResults.getBuffers();
        int[] expected1 = {1, 3, 2, 4};
        int[] expected2 = {3, 3, 3, 3};
        int[] actual1 = new int[omResults.getLength()];
        int[] actual2 = new int[omResults.getLength()];
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < omResults.getLength(); i++) {
            actual1[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < omResults.getLength(); i++) {
            actual2[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(actual1, expected1);
        Assert.assertEquals(actual2, expected2);
        Assert.assertEquals(key, omResults.getKey());
    }

    @Test
    public void testATwoColumn2() {
        JniWrapper jniWrapper = new JniWrapper();
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
        int[] types = {1, 1};
        int[] outputTypes = {1, 1};
        int rowNum = 12;

        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        String moduleId = jniWrapper.compile(code);
        System.out.println("moduleId: " + moduleId);
        String key = UUID.randomUUID().toString();
        OMResult omResults = jniWrapper.execute(moduleId, key, buffers, types, rowNum, outputTypes);
        ByteBuffer[] results = omResults.getBuffers();
        int[] expected1 = {1, 3, 2, 4};
        int[] expected2 = {3, 3, 3, 3};
        int[] actual1 = new int[omResults.getLength()];
        int[] actual2 = new int[omResults.getLength()];
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < omResults.getLength(); i++) {
            actual1[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < omResults.getLength(); i++) {
            actual2[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(actual1, expected1);
        Assert.assertEquals(actual2, expected2);
        Assert.assertEquals(key, omResults.getKey());
    }

    @Test
    public void testBenchmarkSum() {

        // current not support the result
        int row_count = 1000;
        int col_count = 1;
        IntVec datas = buildVec(row_count);
        int[] dataInt = new int[row_count];
        for (int i = 0; i < row_count; i++) {
            dataInt[i] = datas.get(i);
        }
        // in java
        long start = System.currentTimeMillis();
        System.out.println("sum in java:" + sumOnHeap(row_count, dataInt));
        long end = System.currentTimeMillis();
        System.out.println("sum in java total time:" + (end - start) + " ms");

        // use code gen
        String code = "|x:vec[i32]| result(for(x, merger[i32,+], |b,i,e| merge(b,e)))";
        JniWrapper omniNativeRuntime = new JniWrapper();
        ByteBuffer[] bufs = new ByteBuffer[1];
        bufs[0] = datas.getData();
        int[] types = buildType(col_count);
        long compileStart = System.currentTimeMillis();
        String executeId = omniNativeRuntime.compile(code);
        long compileEnd = System.currentTimeMillis();
        System.out.println("compile total time is:" + (compileEnd - compileStart) + " ms");
        int[] outputTypes = {1};
        String key = UUID.randomUUID().toString();
        omniNativeRuntime.execute(executeId, key, bufs, types, row_count, outputTypes);
        long executeEnd = System.currentTimeMillis();
        System.out.println("execute total time is:" + (executeEnd - compileEnd) + " ms");
        System.out.println("code gen total time is:" + (executeEnd - compileStart) + " ms");
    }

    //@Test
    public void testBenchmarkGroupByAndSum() {
        int rowNum = 10_000_000;
        long buildStart = System.currentTimeMillis();
        List<TestGroupBy> groupByList = buildKeyAndValue(rowNum, 200000);
        long buildEnd = System.currentTimeMillis();
        System.out.println("build data time:" + (buildEnd - buildStart));

        // in java
        long start = System.currentTimeMillis();
        Map<Integer, Integer> result = groupByList
                .stream()
                .collect(Collectors.groupingBy(obj -> obj.key, Collectors.summingInt(obj -> obj.value)));
        System.out.println("group by sum:" + result.size());
        long end = System.currentTimeMillis();
        System.out.println("compute data length:" + groupByList.size());
        System.out.println("group by and sum in java total time:" + (end - start) + " ms");

        // build vec data
        IntVec v1 = new IntVec(rowNum);
        IntVec v2 = new IntVec(rowNum);
        for (int i = 0; i < groupByList.size(); i++) {
            TestGroupBy kv = groupByList.get(i);
            v1.set(i, kv.key);
            v2.set(i, kv.value);
        }

        ByteBuffer[] inputData = new ByteBuffer[2];
        inputData[0] = v1.getData();
        inputData[1] = v2.getData();
        int[] inputTypes = {1, 1};
        int[] outTypes = {1, 1};
        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";

        // weld IR code gen
        long compileStart = System.currentTimeMillis();
        String executeId = wrapper.compile(code);
        long compileEnd = System.currentTimeMillis();
        String key = UUID.randomUUID().toString();
        System.out.println("compile total time is:" + (compileEnd - compileStart) + " ms");
        OMResult OMResult = wrapper.execute(executeId, key, inputData, inputTypes, rowNum, outTypes);
        long executeEnd = System.currentTimeMillis();
        System.out.println("execute total time is:" + (executeEnd - compileEnd) + " ms");
        System.out.println("code gen total time is:" + (executeEnd - compileStart) + " ms");
        System.out.println("columns:" + OMResult.getBuffers().length + ",rowNum:" + OMResult.getLength());
    }


    //@Test
    public void testGroupByAndSum() {
        int rowNum = 10;
        IntVec v1 = new IntVec(rowNum);
        IntVec v2 = new IntVec(rowNum);
        for (int idx = 0; idx < rowNum; idx++) {
            v1.set(idx, idx % 3);
            v2.set(idx, idx);
        }

        ByteBuffer[] inputData = new ByteBuffer[2];
        inputData[0] = v1.getData();
        inputData[1] = v2.getData();
        int[] inputTypes = {1, 1};
        int[] outTypes = {1, 1};
        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";

        String executeId = wrapper.compile(code);
        System.out.println("moduleId: " + executeId);

        String key = UUID.randomUUID().toString();
        OMResult omResult = wrapper.execute(executeId, key, inputData, inputTypes, rowNum, outTypes);
        int[] expectKeys = {1, 0, 2};
        int[] expectValues = {12, 18, 15};

        Assert.assertEquals(omResult.getBuffers().length, 2);
        //Assert.assertEquals(omResult.getLength(), 3);

        ByteBuffer[] results = omResult.getBuffers();
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualKey = new int[omResult.getLength()];
        for (int i = 0; i < omResult.getLength(); i++) {
            actualKey[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualValue = new int[omResult.getLength()];
        for (int i = 0; i < omResult.getLength(); i++) {
            actualValue[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(expectKeys, actualKey);
        Assert.assertEquals(expectValues, actualValue);
        Assert.assertEquals(key, omResult.getKey());
    }

    //@Test
    public void testGroupByAndSumWihMultiStep() {
        int rowNum = 10;
        IntVec v1 = new IntVec(rowNum);
        IntVec v2 = new IntVec(rowNum);
        for (int idx = 0; idx < rowNum; idx++) {
            v1.set(idx, idx % 3);
            v2.set(idx, idx);
        }

        ByteBuffer[] inputData = new ByteBuffer[2];
        inputData[0] = v1.getData();
        inputData[1] = v2.getData();
        int[] inputTypes = {1, 1};
        int[] outTypes = {1, 1};
        int[] outTypes2 = {1, 1};
        String code = "|v0 :vec[vec[i32]], v1: vec[vec[i32]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";

        String key1 = "123";
        String executeId = wrapper.compile(code);
        //intermediate
        OMResult omResult1 = wrapper.execute(executeId, key1, inputData, inputTypes, rowNum, outTypes);

        // final
        int[] anotheroutput = {1,1};
        OMResult omResult2 = wrapper.getFinalResult(key1, anotheroutput);

        int[] expectKeys = {1, 0, 2};
        int[] expectValues = {12, 18, 15};

        Assert.assertEquals(omResult2.getBuffers().length, 2);
        Assert.assertEquals(omResult2.getLength(), 3);

        ByteBuffer[] results = omResult2.getBuffers();
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualKey = new int[omResult2.getLength()];
        for (int i = 0; i < omResult2.getLength(); i++) {
            actualKey[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualValue = new int[omResult2.getLength()];
        for (int i = 0; i < omResult2.getLength(); i++) {
            actualValue[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(expectKeys, actualKey);
        Assert.assertEquals(expectValues, actualValue);
        Assert.assertEquals(key1, omResult1.getKey());
    }

    //@Test
    public void testFreeMem() {
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
        omniRuntime.execute(neid, "TMP", vecs, rowNum, outputTypes, OmniOpStep.FINAL);


        int[] value0_ = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] value1_ = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        ByteBuffer[] buffers_ = new ByteBuffer[2];
        IntVec v0_ = new IntVec(20);
        for (int i = 0; i < value0_.length; i++) {
            v0_.set(i, value0_[i]);
        }
        IntVec v1_ = new IntVec(20);
        for (int i = 0; i < value1_.length; i++) {
            v1_.set(i, value1_[i]);
        }
        buffers_[0] = v0_.getData();
        buffers_[1] = v1_.getData();
        System.out.println(v0_.getData().limit());
        String neid_ = omniRuntime.compile(code);
        Vec[] vecs_ = {v0_, v1_};

        Assert.assertEquals(1, (int) v0_.get(0));
        omniRuntime.execute(neid_, "TMP", vecs_, rowNum, outputTypes, OmniOpStep.FINAL);
        //if vector memory free ,get idx value maybe not equal default value
        Assert.assertNotEquals(1, (int) v0_.get(0));
    }

    private List<TestGroupBy> buildKeyAndValue(int rowNum, int distinctCount) {
        List<TestGroupBy> values = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < rowNum; i++) {
//            int key = random.nextInt(objNum);
//            int value = random.nextInt(row);
            TestGroupBy groupBy = new TestGroupBy(i % distinctCount, i);
            values.add(groupBy);
        }
        return values;
    }

    private int[] buildType(int columnNum) {
        int[] types = new int[columnNum];
        for (int i = 0; i < columnNum; i++) {
            types[i] = 1;
        }
        return types;
    }

    private IntVec buildVec(int rowNum) {
        IntVec vec = new IntVec(rowNum);
        Random random = new Random();
        for (int i = 0; i < rowNum; i++) {
            int score = random.nextInt(2000000);
            vec.set(i, score);
        }

        return vec;
    }

    private int sumOnHeap(int rowNum, int[] datas) {
        int sum = 0;
        for (int i = 0; i < rowNum; i++) {
            sum += datas[i];
        }
        return sum;
    }

    private int sumOffHeap(int rowNum, IntVec vec) {
        int sum = 0;
        long time = 0;
        for (int i = 0; i < rowNum; i++) {
            sum += vec.get(i);
        }
        System.out.println("get value total time from off_head:" + time);
        return sum;
    }

    private List<IntVec> buildVec(long rowNum) {
        List<IntVec> vecs = new ArrayList<>();
        IntVec vec1 = new IntVec(10);
        vec1.set(0, 1);
        vec1.set(1, 2);
        vec1.set(2, 3);
        vecs.add(vec1);

        IntVec vec2 = new IntVec(10);
        vec2.set(0, 11);
        vec2.set(1, 22);
        vec2.set(2, 33);
        vecs.add(vec2);

        IntVec vec3 = new IntVec(10);
        vec3.set(0, 100);
        vec3.set(1, 90);
        vec3.set(2, 60);
        vecs.add(vec3);

        return vecs;
    }

    static class TestGroupBy {
        int key;
        int value;

        public TestGroupBy(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }
}
