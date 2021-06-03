package nova.hetu.omniruntime.operator.filter;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OmniFilterAndProjectOperatorFactory
        extends OmniOperatorFactory
{
    private VecType[] inputTypes;
    private String expression;
    private int[] projectIndices;

    public OmniFilterAndProjectOperatorFactory(
            String expression,
            VecType[] inputTypes,
            int[] projectIndices)
    {
        this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
        this.expression = requireNonNull(expression, "Expression is null.");
        this.projectIndices = requireNonNull(projectIndices, "Project indices is null.");
    }

    @Override
    protected long createNativeOperatorFactory()
    {
        return createFilterAndProjectOperatorFactory(
                OmniUtils.transformVecType(inputTypes),
                inputTypes.length,
                expression,
                projectIndices,
                projectIndices.length);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, inputTypes, projectIndices);
    }

    private static native long createFilterAndProjectOperatorFactory(int[] inputTypes, int inputLength, String expression, int[] projectIndices, int projectLength);
}
