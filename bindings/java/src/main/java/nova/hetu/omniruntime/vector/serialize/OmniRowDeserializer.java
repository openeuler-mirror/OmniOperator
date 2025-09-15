/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package nova.hetu.omniruntime.vector.serialize;

/**
 * VecBatchSerializer implementation of protobuf.
 *
 * @since 2024-05-16
 */
public class OmniRowDeserializer {
    private long nativeParser = 0L;

    public OmniRowDeserializer(int[] types, long[] vecs) {
        nativeParser = newOmniRowDeserializer(types, vecs);
    }

    public long getNativeParser() {
        return nativeParser;
    }

    private static native long newOmniRowDeserializer(int[] types, long[] vecs);

    private static native long freeOmniRowDeserializer(long addr);

    private static native void parseOneRow(long nativeParserAddr, byte[] bytes, int rowIdx);

    private static native void parseOneRowByAddr(long nativeParserAddr, long rowAddress, int rowIdx);

    private static native void parseAllRow(long nativeParserAddr, long rowBatchAddr);

    /**
     * used when shuffle read
     *
     * @param address one row 's bytes
     * @param rowIdx vector 's index
     */
    public void parse(long address, int rowIdx) {
        parseOneRowByAddr(getNativeParser(), address, rowIdx);
    }

    /**
     * used to parse native row to vector batch.
     *
     * @param rowBatchAddr address of native row batch
     */
    public void parseAll(long rowBatchAddr) {
        parseAllRow(getNativeParser(), rowBatchAddr);
    }

    /**
     * deserializer is created in native side, we must free it after we use.
     */
    public void close() {
        freeOmniRowDeserializer(nativeParser);
    }
}
