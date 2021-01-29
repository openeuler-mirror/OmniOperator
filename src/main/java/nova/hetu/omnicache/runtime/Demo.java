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
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.nio.ByteBuffer;

public class Demo {
    public static void main(String[] args)
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
        String code = "|v0 :vec[i32], v1: vec[i32]|" +
                "let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i32,+], |b,i,n| merge(b, {n.$0, n.$1}))));" +
                "let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));" +
                "let v = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$1)));" +
                "{k,v}";
        OmniRuntime omniRuntime = new OmniRuntime();
        String neid = omniRuntime.compile(code);
        Vec[] vecs = {v0, v1};
        Vec[] resultVecs = omniRuntime.execute(neid, vecs, rowNum, outputTypes);
        for(int i=0;i<resultVecs[0].size();i++) {
            StringBuilder sb = new StringBuilder();
            for (int idx = 0; idx < resultVecs.length; idx++) {
                sb.append(((IntVec) resultVecs[idx]).get(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
}
