package nova.hetu.omniruntime.operator.window;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.WindowFunctionType;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Arrays;
import java.util.Objects;

public class OmniWindowOperatorFactory
        extends OmniOperatorFactory<OmniWindowOperatorFactory.Context>
{
    public OmniWindowOperatorFactory(
            VecType[] sourceTypes,
            int[] outputChannels,
            WindowFunctionType[] windowFunction,
            int[] partitionChannels,
            int[] preGroupedChannels,
            int[] sortChannels,
            int[] sortOrder,
            int[] sortNullFirsts,
            int preSortedChannelPrefix,
            int expectedPositions,
            int[] argumentChannels,
            VecType[] windowFunctionReturnType)
    {
        super(new Context(sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels, windowFunctionReturnType));
    }

    private static native long createWindowOperatorFactory(int[] sourceTypes, int[] outputChannels, int[] windFunction, int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder, int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels, int[] windowFunctionReturnType);

    @Override
    protected long createNativeOperatorFactory(Context context)
    {
        return createWindowOperatorFactory(
                OmniUtils.transformVecType(context.sourceTypes),
                context.outputChannels,
                OmniUtils.transformWindowFunctionType(context.windFunction),
                context.partitionChannels,
                context.preGroupedChannels,
                context.sortChannels,
                context.sortOrder,
                context.sortNullFirsts,
                context.preSortedChannelPrefix,
                context.expectedPositions,
                context.argumentChannels,
                OmniUtils.transformVecType(context.windowFunctionReturnType));
    }

    public static class Context
            extends OmniOperatorFactoryContext
    {
        private final VecType[] sourceTypes;
        private final int[] outputChannels;
        private final WindowFunctionType[] windFunction;
        private final int[] partitionChannels;
        private final int[] sortChannels;
        private final int[] sortOrder;
        private int[] preGroupedChannels;
        private int[] sortNullFirsts;
        private int preSortedChannelPrefix;
        private int expectedPositions;
        private int[] argumentChannels;
        private VecType[] windowFunctionReturnType;

        public Context(
                VecType[] sourceTypes,
                int[] outputChannels,
                WindowFunctionType[] windowFunction,
                int[] partitionChannels,
                int[] preGroupedChannels,
                int[] sortChannels,
                int[] sortOrder,
                int[] sortNullFirsts,
                int preSortedChannelPrefix,
                int expectedPositions,
                int[] argumentChannels,
                VecType[] windowFunctionReturnType)
        {
            this.sourceTypes = sourceTypes;
            this.outputChannels = outputChannels;
            this.windFunction = windowFunction;
            this.partitionChannels = partitionChannels;
            this.preGroupedChannels = preGroupedChannels;
            this.sortChannels = sortChannels;
            this.sortOrder = sortOrder;
            this.sortNullFirsts = sortNullFirsts;
            this.preSortedChannelPrefix = preSortedChannelPrefix;
            this.expectedPositions = expectedPositions;
            this.argumentChannels = argumentChannels;
            this.windowFunctionReturnType = windowFunctionReturnType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceTypes, outputChannels, windFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels, windowFunctionReturnType);
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
            if (!super.equals(o)) {
                return false;
            }
            Context context = (Context) o;
            return preSortedChannelPrefix == context.preSortedChannelPrefix && expectedPositions == context.expectedPositions && Arrays.equals(sourceTypes, context.sourceTypes) && Arrays.equals(outputChannels, context.outputChannels) && Arrays.equals(windFunction, context.windFunction) && Arrays.equals(partitionChannels, context.partitionChannels) && Arrays.equals(preGroupedChannels, context.preGroupedChannels) && Arrays.equals(sortChannels, context.sortChannels) && Arrays.equals(sortOrder, context.sortOrder) && Arrays.equals(sortNullFirsts, context.sortNullFirsts) && Arrays.equals(argumentChannels, context.argumentChannels) && Arrays.equals(windowFunctionReturnType, context.windowFunctionReturnType);
        }
    }
}
