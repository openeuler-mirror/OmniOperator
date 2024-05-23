/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package nova.hetu.omniruntime.vector.serialize;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;

public class OmniRowDeserializer {
    private long nativeParser = 0;
    public OmniRowDeserializer(int[] types) {
        nativeParser = newOmniRowDeserializer(types);
    }

    public long getNativeParser() {
        return nativeParser;
    }

    public static native long newOmniRowDeserializer(int[] types);
    public static native long freeOmniRowDeserializer(long addr);

    public static native void parseOneRow(long nativeParserAddr, byte[] bytes, long[] vecs, int rowIdx);

    public void parse(byte[] bytes, long[] vecs, int rowIdx) {
        parseOneRow(getNativeParser(), bytes, vecs, rowIdx);
    }

    public void close(){
        freeOmniRowDeserializer(nativeParser);
    }
}
