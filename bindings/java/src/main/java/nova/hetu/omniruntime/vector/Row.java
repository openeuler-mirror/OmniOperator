package nova.hetu.omniruntime.vector;

import java.io.Closeable;
import java.io.IOException;

public class Row implements Closeable {

    protected final long nativeRow;


    public Row(long dataAddr, int hashPos, int len) {
        nativeRow = dataAddr;
        keyPos = hashPos;
        length = len;
        this.rowBuf = OmniBufferFactory.create(dataAddr, len);
    }

    public int getKeyPos() {
        return keyPos;
    }

    public byte[] getKey() {
        return rowBuf.getBytes(0, keyPos);
    }

    public int getCapacity() {
        return rowBuf.getCapacity();
    }

    public int getLength() {
        return length;
    }

    public long getNativeRow() {
        return nativeRow;
    }
    @Override
    public void close() {

    }

    private int keyPos;

    private int length;

    private OmniBuffer rowBuf;
}