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

import nova.hetu.omnicache.vector.AggType;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

public class OmniRuntimeTest
{
    private Vec[] builder4LongColumnRawData()
    {
        LongVec key1 = new LongVec(1);
        LongVec key2 = new LongVec(1);
        LongVec value1 = new LongVec(1);
        LongVec value2 = new LongVec(1);

        key1.set(0, 1);
        key2.set(0, 1);
        value1.set(0, 1);
        value2.set(0, 1);
        return new Vec[] {key1, key2, value1, value2};
    }

    @Test
    public void test_multi_thread_2c_groupby_and_2c_sum()
    {
        final String long_2c_group_and_2c_sum_weld_ir_code = "|v0 :vec[vec[i64]], v1: vec[vec[i64]], v2: vec[vec[i64]], v3: vec[vec[i64]]|" +
                "let sum_dict_ = for(zip(v0, v1, v2, v3), dictmerger[{i64,i64}, {i64, i64},+], |b,i,n| " +
                "for(zip(n.$0, n.$1, n.$2, n.$3), b, |b_, i_, m|" +
                "merge(b, {{m.$0, m.$1}, {m.$2, m.$3}})));" +
                "let dict_0_1 = tovec(result(sum_dict_));" +
                "let k0 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$0)));" +
                "let k1 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$1)));" +
                "let sum_1 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$1.$0)));" +
                "let sum_2 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$1.$1)));" +
                "{k0, k1, sum_1, sum_2}";
        final OmniRuntime omniRuntime = new OmniRuntime();
        final VecType[] inputDataTypes = new VecType[] {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
        final int inputPageSize = 1;
        int threadCount = 1000;
        int pageCount = 1000;

        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        boolean[] asyncResult = new boolean[threadCount];
        for (int tidx = 0; tidx < threadCount; tidx++) {
            final int aIdx = tidx;
            Thread worker = new Thread(() -> {
                try {
                    String nativeExecId = omniRuntime.compile(long_2c_group_and_2c_sum_weld_ir_code);
                    String omniKey = UUID.randomUUID().toString();
                    for (int pidx = 1; pidx <= pageCount; pidx++) {
                        Vec[] input = builder4LongColumnRawData();
                        Vec[] intermediateResult = (Vec[]) omniRuntime.execute(nativeExecId, omniKey, input, inputPageSize, inputDataTypes, OmniOpStep.INTERMEDIATE);
                        checkGroupBy2CAndSum3CDataValid(intermediateResult, pidx);
                    }
                    Vec[] result = (Vec[]) omniRuntime.getResults(omniKey, inputDataTypes);
                    checkGroupBy2CAndSum3CDataValid(result, pageCount);
                    if (4 == result.length &&
                            1 == ((LongVec) result[0]).get(0) &&
                            1 == ((LongVec) result[1]).get(0) &&
                            pageCount == ((LongVec) result[2]).get(0) &&
                            pageCount == ((LongVec) result[3]).get(0)) {
                        asyncResult[aIdx] = true;
                    }else{
                        asyncResult[aIdx] = false;
                    }
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            worker.setName("thread-" + tidx);
            worker.start();
        }
        try {
            countDownLatch.await();
            for (int i = 0; i < threadCount; i++) {
                Assert.assertEquals(true, asyncResult[i]);
            }
        }catch (InterruptedException ex){
            Assert.assertEquals(true,false);
        }
    }

    private void checkGroupBy2CAndSum3CDataValid(Vec[] result, long expected)
    {
        if (result == null || result.length != 4) {
            System.out.println("result struct is invalid!");
        }
        else if (((LongVec) result[0]).get(0) != 1 || ((LongVec) result[1]).get(0) != 1 || ((LongVec) result[2]).get(0) != expected || ((LongVec) result[3]).get(0) != expected) {
            String msg = format("[%s]invalid intermediate result:key1=%s,key2=%s,value1=%s,value2=%s,expected=%s", Thread.currentThread().getName(), ((LongVec) result[0]).get(0), ((LongVec) result[1]).get(0), ((LongVec) result[2]).get(0), ((LongVec) result[3]).get(0), expected);
            System.out.println(msg);
        }
    }
    @Test
    public void test_multi_thread_1c_groupby_1c_sum(){
        final String long_1c_group_and_1c_sum_weld_ir_code = "|v0 :vec[vec[i64]], v1: vec[vec[i64]]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i64,i64,+], |b,i,n| for(zip(n.$0, n.$1), b, |b_, i_, m| " + "merge(b, {m.$0, m.$1})))));" +
                "let k = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        final OmniRuntime omniRuntime = new OmniRuntime();
        final VecType[] inputDataTypes = new VecType[] {VecType.LONG, VecType.LONG};
        final int inputPageSize = 1;
        int threadCount = 1000;
        int pageCount = 1000;

        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        boolean[] asyncResult = new boolean[threadCount];
        for (int tidx = 0; tidx < threadCount; tidx++) {
            final int aIdx = tidx;
            Thread worker = new Thread(() -> {
                try {
                    String nativeExecId = omniRuntime.compile(long_1c_group_and_1c_sum_weld_ir_code);
                    String omniKey = UUID.randomUUID().toString();

                    for (int pidx = 1; pidx <= pageCount; pidx++) {
                        Vec[] input = builder2LongColumnRawData();
                        Vec[] intermediateResult = (Vec[]) omniRuntime.execute(nativeExecId, omniKey, input, inputPageSize, inputDataTypes, OmniOpStep.INTERMEDIATE);
                    }
                    Vec[] result = (Vec[]) omniRuntime.getResults(omniKey, inputDataTypes);
                    if (2 == result.length &&
                            1 == ((LongVec) result[0]).get(0) &&
                            pageCount == ((LongVec) result[1]).get(0)) {
                        asyncResult[aIdx] = true;
                    }else{
                        asyncResult[aIdx] = false;
                    }
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            worker.setName("thread-" + tidx);
            worker.start();
        }
        try {
            countDownLatch.await();
            for (int i = 0; i < threadCount; i++) {
                Assert.assertEquals(true, asyncResult[i]);
            }
        }catch (InterruptedException ex){
            Assert.assertEquals(true,false);
        }
    }
    public Vec[] builder2LongColumnRawData(){
        LongVec key1 = new LongVec(1);
        LongVec value1 = new LongVec(1);

        key1.set(0, 1);
        value1.set(0, 1);
        return new Vec[] {key1, value1};
    }

    @Test
    public void testMemoryFee() {
        LongVec v1 = new LongVec(1024);
        v1.set(0, 1000);
        int count = 10;
        for (int i = 0;i < count;i++) {
            v1.close();
        }
    }

    @Test
    public void testExecuteAggOnePage() {
        long operatorId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        long stageId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        int totalChannel = 2;
        int[] groupByChanel = {0};
        VecType[] groupByTypes = {VecType.LONG};
        int[] aggChannels = {1};
        VecType[] aggTypes = {VecType.LONG};
        AggType[] aggFunctionTypes = {AggType.SUM};
        VecType[] aggOutputTypes = {VecType.LONG, VecType.LONG};
        VecType[] inputTypes = {VecType.LONG, VecType.LONG};
        OmniRuntime omniRuntime = new OmniRuntime();

        omniRuntime.prepareAgg(
                stageId,
                operatorId,
                totalChannel,
                groupByChanel,
                groupByTypes,
                aggChannels,
                aggTypes,
                aggFunctionTypes,
                aggOutputTypes,
                inputTypes);

        int rowNum = 10;
        List<Vec> inputData = build2Columns(rowNum);

        omniRuntime.executeAggIntermediate(stageId, operatorId, inputData, 2);
        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        Vec[] result  = omniRuntime.executeAggFinal(operatorId, aggOutputTypes);
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(((LongVec)result[0]).get(0), 0);
        Assert.assertEquals(((LongVec)result[1]).get(0), rowNum);
        // release result memory
        releaseVecMemory(result);
    }

    @Test
    public void testExecuteAggMultiplePage() {
        long stageId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        long operatorId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        int totalChannel = 4;
        int[] groupByChanel = {0, 1};
        VecType[] groupByTypes = {VecType.LONG, VecType.LONG};
        int[] aggChannels = {2, 3};
        VecType[] aggTypes = {VecType.LONG, VecType.LONG};
        AggType[] aggFunctionTypes = {AggType.SUM, AggType.SUM};
        VecType[] aggOutputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
        OmniRuntime omniRuntime = new OmniRuntime();
        VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
        omniRuntime.prepareAgg(
                stageId,
                operatorId,
                totalChannel,
                groupByChanel,
                groupByTypes,
                aggChannels,
                aggTypes,
                aggFunctionTypes,
                aggOutputTypes,
                inputTypes);
        int rowNum = 100;
        int pageCount = 10;

        List<Vec> inputData = new ArrayList<>();
        for (int i = 0; i < pageCount;i++) {
            inputData.addAll(build4Columns(rowNum));
        }

        omniRuntime.executeAggIntermediate(stageId, operatorId, inputData, 4);

        // release input data memory
        releaseVecMemory(inputData.toArray(new Vec[0]));

        Vec[] results  = omniRuntime.executeAggFinal(operatorId, aggOutputTypes);
        Assert.assertEquals(results.length, 4);
        Assert.assertEquals(((LongVec)results[0]).get(0), 0);
        Assert.assertEquals(((LongVec)results[1]).get(0), 0);
        Assert.assertEquals(((LongVec)results[2]).get(0), rowNum * pageCount);
        Assert.assertEquals(((LongVec)results[3]).get(0), rowNum * pageCount);

        // release result memory
        releaseVecMemory(results);
    }

    private void releaseVecMemory(Vec[] vecs) {
        for (Vec vec: vecs) {
            vec.close();
        }
    }

    @Test
    public void testExecuteAggMultipleThread() {
        int pageCount = 10;
        int threadCount = 10;
        int rowNum = 100;
        multiThreadExecution(threadCount,rowNum, pageCount);
    }

    private void multiThreadExecution( int threadCount, int rowNum, int pageCount)
    {
        CountDownLatch downLatch = new CountDownLatch(threadCount);
        for (int tIdx = 0; tIdx < threadCount; tIdx++) {
            Thread thread = new Thread(() -> {
                try {
                    long stageId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
                    long operatorId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
                    int totalChannel = 4;
                    int[] groupByChanel = {0, 1};
                    VecType[] groupByTypes = {VecType.LONG, VecType.LONG};
                    int[] aggChannels = {2, 3};
                    VecType[] aggTypes = {VecType.LONG, VecType.LONG};
                    AggType[] aggFunctionTypes = {AggType.SUM, AggType.SUM};
                    VecType[] aggOutputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};
                    VecType[] inputTypes = {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};

                    OmniRuntime omniRuntime = new OmniRuntime();
                    omniRuntime.prepareAgg(
                            stageId,
                            operatorId,
                            totalChannel,
                            groupByChanel,
                            groupByTypes,
                            aggChannels,
                            aggTypes,
                            aggFunctionTypes,
                            aggOutputTypes,
                            inputTypes);

                    List<Vec> inputData = new ArrayList<>();
                    for (int i = 0; i < pageCount;i++) {
                        inputData.addAll(build4Columns(rowNum));
                    }

                    omniRuntime.executeAggIntermediate(stageId, operatorId, inputData, 4);

                    // release input data memory
                    releaseVecMemory(inputData.toArray(new Vec[0]));

                    Vec[] result  = omniRuntime.executeAggFinal(operatorId, aggOutputTypes);
                    Assert.assertEquals(result.length, 4);
                    Assert.assertEquals(((LongVec)result[0]).get(0), 0);
                    Assert.assertEquals(((LongVec)result[1]).get(0), 0);
                    Assert.assertEquals(((LongVec)result[2]).get(0), rowNum * pageCount);
                    Assert.assertEquals(((LongVec)result[3]).get(0), rowNum * pageCount);

                    // release result memory
                    releaseVecMemory(result);
                }
                finally {
                    downLatch.countDown();
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

    private List<Vec> build4Columns(int rowNum) {
        List<Vec> columns = new ArrayList<>();

        LongVec c1 = new LongVec(rowNum);
        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 0);
            c2.set(i, 0);
        }
        columns.add(c1);
        columns.add(c2);

        LongVec c3 = new LongVec(rowNum);
        LongVec c4 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c3.set(i, 1);
            c4.set(i, 1);
        }
        columns.add(c3);
        columns.add(c4);

        return columns;
    }

    private List<Vec> build2Columns(int rowNum) {
        List<Vec> columns = new ArrayList<>();

        LongVec c1 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c1.set(i, 0);
        }
        columns.add(c1);

        LongVec c2 = new LongVec(rowNum);
        for (int i = 0; i < rowNum; i++) {
            c2.set(i, 1);
        }
        columns.add(c2);

        return columns;
    }
}
