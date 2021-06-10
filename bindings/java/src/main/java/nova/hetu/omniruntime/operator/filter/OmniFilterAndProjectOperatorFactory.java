package nova.hetu.omniruntime.operator.filter;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniFilterAndProjectOperatorFactory
        extends OmniOperatorFactory<OmniFilterAndProjectOperatorFactory.Context>
{
    public OmniFilterAndProjectOperatorFactory(
            String expression,
            VecType[] inputTypes,
            int[] projectIndices)
    {
        super(new Context(expression, inputTypes, projectIndices));
    }

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createFilterAndProjectOperatorFactory(
                OmniUtils.transformVecType(context.inputTypes),
                context.inputTypes.length,
                context.expression,
                context.projectIndices,
                context.projectIndices.length);
    }

    private static native long createFilterAndProjectOperatorFactory(int[] inputTypes, int inputLength, String expression, int[] projectIndices, int projectLength);

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] inputTypes;
        private final String expression;
        private final int[] projectIndices;

        public Context(
                String expression,
                VecType[] inputTypes,
                int[] projectIndices)
        {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expression = requireNonNull(expression, "Expression is null.");
            this.projectIndices = requireNonNull(projectIndices, "Project indices is null.");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, inputTypes, projectIndices);
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
            return Objects.equals(expression, that.expression)
                    && Objects.equals(inputTypes, that.inputTypes)
                    && Objects.equals(projectIndices, that.projectIndices);
        }
    }
}
