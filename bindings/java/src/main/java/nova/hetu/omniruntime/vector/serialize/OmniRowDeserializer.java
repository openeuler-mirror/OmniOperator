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

    public OmniRowDeserializer(int[] types) {
        nativeParser = newOmniRowDeserializer(types);
    }

    public long getNativeParser() {
        return nativeParser;
    }

    private static native long newOmniRowDeserializer(int[] types);

    private static native long freeOmniRowDeserializer(long addr);

    private static native void parseOneRow(long nativeParserAddr, byte[] bytes, long[] vecs, int rowIdx);

    private static native void parseOneRowByAddr(long nativeParserAddr, long rowAddress, long[] vecs, int rowIdx);

    private static native void parseAllRow(long nativeParserAddr, long rowBatchAddr, long[] vecs);

    /**
     * used when shuffle read
     *
     * @param bytes one row 's bytes
     * @param vecs all vector
     * @param rowIdx vector 's index
     */
    public void parse(byte[] bytes, long[] vecs, int rowIdx) {
        parseOneRow(getNativeParser(), bytes, vecs, rowIdx);
    }

    /**
     * used to parse native row to vector batch.
     *
     * @param rowBatchAddr address of native row batch
     * @param vecs vec array
     */
    public void parseAll(long rowBatchAddr, long[] vecs) {
        parseAllRow(getNativeParser(), rowBatchAddr, vecs);
    }

    /**
     * deserializer is created in native side, we must free it after we use.
     */
    public void close() {
        freeOmniRowDeserializer(nativeParser);
    }
}
