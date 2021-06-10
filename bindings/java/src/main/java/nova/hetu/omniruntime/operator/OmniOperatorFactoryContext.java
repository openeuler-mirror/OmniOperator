package nova.hetu.omniruntime.operator;

public abstract class OmniOperatorFactoryContext
{
    @Override
    public int hashCode()
    {
        throw new RuntimeException("Unsupported hashCode");
    }

    @Override
    public boolean equals(Object that)
    {
        throw new RuntimeException("Unsupported equals");
    }
}
