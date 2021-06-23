package nova.hetu.omniruntime.constants;

import nova.hetu.omniruntime.OmniLibs;

import java.util.Objects;

@SuppressWarnings("StaticVariableName")
public abstract class Constant
{
    private final int value;

    public Constant(int value)
    {
        this.value = value;
    }

    static {
        OmniLibs.load();
        loadConstants();
    }

    public int getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return ((Constant) o).getValue() == value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    private static native void loadConstants();
}
