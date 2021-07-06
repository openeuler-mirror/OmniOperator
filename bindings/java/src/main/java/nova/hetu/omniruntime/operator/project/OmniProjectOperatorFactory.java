package nova.hetu.omniruntime.operator.project;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

public class OmniProjectOperatorFactory
        extends OmniOperatorFactory<OmniProjectOperatorFactory.Context>
{
    public OmniProjectOperatorFactory(
            String[] expressions,
            VecType[] inputTypes)
    {
        super(new Context(expressions, inputTypes));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createProjectOperatorFactory(
                toNativeConstants(context.inputTypes),
                context.inputTypes.length,
                context.expressions,
                context.expressions.length);
    }

    private static native long createProjectOperatorFactory(int[] inputTypes, int inputLength, Object[] expressions, int expressionsLength);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] inputTypes;
        private final String[] expressions;

        public Context(
                String[] expressions,
                VecType[] inputTypes)
        {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expressions = requireNonNull(expressions, "Expressions is null.");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expressions, inputTypes);
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
            Context that = (Context) o;
            return Objects.equals(expressions, that.expressions)
                    && Objects.equals(inputTypes, that.inputTypes);
        }
    }
}
