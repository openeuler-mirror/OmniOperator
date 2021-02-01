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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class JniWrapperTest {

    JniWrapper wrapper;
    private final static String key = "UNKNOW";

    @BeforeClass
    public void setUp() {
        wrapper = new JniWrapper();
    }

    @Test
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
        String code = "|x:vec[i32]| len(x)";
        String moduleId = wrapper.compile(code);
        // current not support the result
        wrapper.execute(moduleId, key, buffers, types, rowNum, types, OmniOpStep.FINAL.getState());
    }

    @Test
    public void testTwoColumn() {
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

        String code = "|v0 :vec[i32], v1: vec[i32]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| merge(b, {n.$0, n.$1}))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        String moduleId = jniWrapper.compile(code);
        OMResult OMResults = jniWrapper.execute(moduleId, key, buffers, types, rowNum, outputTypes, OmniOpStep.FINAL.getState());
        ByteBuffer[] results = OMResults.getBuffers();
        int[] expected1 = {1, 3, 2, 4};
        int[] expected2 = {3, 3, 3, 3};
        int[] actual1 = new int[OMResults.getLength()];
        int[] actual2 = new int[OMResults.getLength()];
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < OMResults.getLength(); i++) {
            actual1[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < OMResults.getLength(); i++) {
            actual2[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(expected1, actual1);
        Assert.assertEquals(expected2, actual2);

    }

    @Test
    public void testBenchmarkSum() {

        // current not support the result
        int rowNum = 1000;
        IntVec datas = buildVec(rowNum);
        int[] dataInt = new int[rowNum];
        for (int i = 0; i < rowNum; i++) {
            dataInt[i] = datas.get(i);
        }
        // in java
        long start = System.currentTimeMillis();
        System.out.println("sum in java:" + sumOnHeap(rowNum, dataInt));
        long end = System.currentTimeMillis();
        System.out.println("sum in java total time:" + (end - start) + " ms");

        // use code gen
        String code = "|x:vec[i32]| result(for(x, merger[i32,+], |b,i,e| merge(b,e)))";
        JniWrapper omniNativeRuntime = new JniWrapper();
        ByteBuffer[] bufs = new ByteBuffer[1];
        bufs[0] = datas.getData();
        int[] types = buildType(rowNum);
        long compileStart = System.currentTimeMillis();
        String executeId = omniNativeRuntime.compile(code);
        long compileEnd = System.currentTimeMillis();
        System.out.println("compile total time is:" + (compileEnd - compileStart) + " ms");
        int[] outputTypes = {1};
        omniNativeRuntime.execute(executeId, key, bufs, types, rowNum, outputTypes, OmniOpStep.FINAL.getState());
        long executeEnd = System.currentTimeMillis();
        System.out.println("execute total time is:" + (executeEnd - compileEnd) + " ms");
        System.out.println("code gen total time is:" + (executeEnd - compileStart) + " ms");
    }

    @Test
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
        String code = "|k:vec[i32],v:vec[i32]|" +
                "let rs = tovec(result(for(zip(k,v),dictmerger[i32,i32,+],|b,i,n| merge(b,{n.$0,n.$1}))));" +
                "let k = result(for(rs,appender[i32],|b,i,n| merge(b,n.$0)));" +
                "let v = result(for(rs,appender[i32],|b,i,n| merge(b,n.$1)));" +
                "{k,v}";

        // weld IR code gen
        long compileStart = System.currentTimeMillis();
        String executeId = wrapper.compile(code);
        long compileEnd = System.currentTimeMillis();
        System.out.println("compile total time is:" + (compileEnd - compileStart) + " ms");
        OMResult OMResult = wrapper.execute(executeId, key, inputData, inputTypes, rowNum, outTypes, OmniOpStep.FINAL.getState());
        long executeEnd = System.currentTimeMillis();
        System.out.println("execute total time is:" + (executeEnd - compileEnd) + " ms");
        System.out.println("code gen total time is:" + (executeEnd - compileStart) + " ms");
        System.out.println("columns:" + OMResult.getBuffers().length + ",rowNum:" + OMResult.getLength());
    }

    @Test
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
        String code = "|k:vec[i32],v:vec[i32]|" +
                "let rs = tovec(result(for(zip(k,v),dictmerger[i32,i32,+],|b,i,n| merge(b,{n.$0,n.$1}))));" +
                "let k = result(for(rs,appender[i32],|b,i,n| merge(b,n.$0)));" +
                "let v = result(for(rs,appender[i32],|b,i,n| merge(b,n.$1)));" +
                "{k,v}";

        String executeId = wrapper.compile(code);
        OMResult OMResult = wrapper.execute(executeId, key, inputData, inputTypes, rowNum, outTypes, OmniOpStep.FINAL.getState());
        int[] expectKeys = {1, 0, 2};
        int[] expectValues = {12, 18, 15};

        Assert.assertEquals(OMResult.getBuffers().length, 2);
        Assert.assertEquals(OMResult.getLength(), 3);

        ByteBuffer[] results = OMResult.getBuffers();
        results[0].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualKey = new int[OMResult.getLength()];
        for (int i = 0; i < OMResult.getLength(); i++) {
            actualKey[i] = results[0].getInt(i * Integer.BYTES);
        }

        results[1].order(ByteOrder.LITTLE_ENDIAN);
        int[] actualValue = new int[OMResult.getLength()];
        for (int i = 0; i < OMResult.getLength(); i++) {
            actualValue[i] = results[1].getInt(i * Integer.BYTES);
        }

        Assert.assertEquals(expectKeys, actualKey);
        Assert.assertEquals(expectValues, actualValue);

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
