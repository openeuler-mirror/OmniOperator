package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_SHORT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;

public class DictionaryVec
        extends Vec
{
    private Vec dictionary;
    private int[] ids;

    public DictionaryVec(long nativeVector)
    {
        super(nativeVector);
        long dictionaryNative = getDictionaryNative(nativeVector);
        VecType type = new VecType(getTypeNative(dictionaryNative));
        if (OMNI_VEC_TYPE_INT.equals(type)) {
            this.dictionary = new IntVec(dictionaryNative);
        }
        else if (OMNI_VEC_TYPE_SHORT.equals(type)) {
            this.dictionary = new ShortVec(dictionaryNative);
        }
        else if (OMNI_VEC_TYPE_LONG.equals(type)) {
            this.dictionary = new LongVec(dictionaryNative);
        }
        else if (OMNI_VEC_TYPE_VARCHAR.equals(type)) {
            this.dictionary = new VarcharVec(dictionaryNative);
        }
        this.ids = getIdsNative(nativeVector);
    }

    public DictionaryVec(Vec dictionary, int[] ids)
    {
        super(dictionary.getNativeVector());
        this.dictionary = dictionary;
        this.ids = ids;
    }

    public Vec getDictionary()
    {
        return dictionary;
    }

    public int[] getIds()
    {
        return ids;
    }

    public int getSize()
    {
        return ids.length;
    }

    public int getInt(int index)
    {
        return ((IntVec) dictionary).get(ids[index]);
    }

    public long getLong(int index)
    {
        return ((LongVec) dictionary).get(ids[index]);
    }

    @Override
    public Vec slice(int start, int length)
    {
        return null;
    }

    @Override
    public Vec copy()
    {
        return null;
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length)
    {
        return null;
    }

    @Override
    public Vec copyRegion(int positionOffset, int length)
    {
        return null;
    }

    private static native long getDictionaryNative(long nativeVector);

    private static native int[] getIdsNative(long nativeVector);
}
