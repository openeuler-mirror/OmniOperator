/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.window;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.WindowFunctionType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni window operator factory.
 *
 * @since 20210630
 */
public class OmniWindowOperatorFactory extends OmniOperatorFactory<OmniWindowOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni window operator factory.
     *
     * @param sourceTypes the source types
     * @param outputChannels the output channels
     * @param windowFunction the window function
     * @param partitionChannels the partition channels
     * @param preGroupedChannels the pre grouped channels
     * @param sortChannels the sort channels
     * @param sortOrder the sort order
     * @param sortNullFirsts the sort null firsts
     * @param preSortedChannelPrefix the pre sorted channel prefix
     * @param expectedPositions the expected positions
     * @param argumentChannels the argument channels
     * @param windowFunctionReturnType the window function return type
     */
    public OmniWindowOperatorFactory(VecType[] sourceTypes, int[] outputChannels, WindowFunctionType[] windowFunction,
        int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder, int[] sortNullFirsts,
        int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels, VecType[] windowFunctionReturnType) {
        super(new FactoryContext(
            new JitContext(sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels,
                sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels,
                windowFunctionReturnType)));
    }

    private static native long createWindowJitContext(String sourceTypes, int[] outputChannels, int[] windFunction,
        int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder, int[] sortNullFirsts,
        int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels, String windowFunctionReturnType);

    private static native long createWindowOperatorFactory(String sourceTypes, int[] outputChannels, int[] windFunction,
        int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder, int[] sortNullFirsts,
        int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels, String windowFunctionReturnType,
        long jitContext);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createWindowOperatorFactory(VecTypeSerializer.serialize(context.sourceTypes), context.outputChannels,
            toNativeConstants(context.windFunction), context.partitionChannels, context.preGroupedChannels,
            context.sortChannels, context.sortOrder, context.sortNullFirsts, context.preSortedChannelPrefix,
            context.expectedPositions, context.argumentChannels,
            VecTypeSerializer.serialize(context.windowFunctionReturnType), factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final VecType[] sourceTypes;

        private final int[] outputChannels;

        private final WindowFunctionType[] windFunction;

        private final int[] partitionChannels;

        private final int[] sortChannels;

        private final int[] sortOrder;

        private final int[] preGroupedChannels;

        private final int[] sortNullFirsts;

        private final int preSortedChannelPrefix;

        private final int expectedPositions;

        private final int[] argumentChannels;

        private final VecType[] windowFunctionReturnType;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param windowFunction the window function
         * @param partitionChannels the partition channels
         * @param preGroupedChannels the pre grouped channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         * @param sortNullFirsts the sort null firsts
         * @param preSortedChannelPrefix the pre sorted channel prefix
         * @param expectedPositions the expected positions
         * @param argumentChannels the argument channels
         * @param windowFunctionReturnType the window function return type
         */
        public JitContext(VecType[] sourceTypes, int[] outputChannels, WindowFunctionType[] windowFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            VecType[] windowFunctionReturnType) {
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
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputChannels),
                Arrays.hashCode(windFunction), Arrays.hashCode(partitionChannels), Arrays.hashCode(preGroupedChannels),
                Arrays.hashCode(sortChannels), Arrays.hashCode(sortOrder), Arrays.hashCode(sortNullFirsts),
                preSortedChannelPrefix, expectedPositions, Arrays.hashCode(argumentChannels),
                Arrays.hashCode(windowFunctionReturnType));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext context = (JitContext) obj;
            return preSortedChannelPrefix == context.preSortedChannelPrefix
                && expectedPositions == context.expectedPositions && Arrays.equals(sourceTypes, context.sourceTypes)
                && Arrays.equals(outputChannels, context.outputChannels) && Arrays.equals(windFunction,
                context.windFunction) && Arrays.equals(partitionChannels, context.partitionChannels) && Arrays.equals(
                preGroupedChannels, context.preGroupedChannels) && Arrays.equals(sortChannels, context.sortChannels)
                && Arrays.equals(sortOrder, context.sortOrder) && Arrays.equals(sortNullFirsts, context.sortNullFirsts)
                && Arrays.equals(argumentChannels, context.argumentChannels) && Arrays.equals(windowFunctionReturnType,
                context.windowFunctionReturnType);
        }
    }

    /**
     * The type Factory context.
     *
     * @since 20210630
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         */
        public FactoryContext(JitContext jitContext) {
            super(jitContext);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            return createWindowJitContext(VecTypeSerializer.serialize(context.sourceTypes), context.outputChannels,
                toNativeConstants(context.windFunction), context.partitionChannels, context.preGroupedChannels,
                context.sortChannels, context.sortOrder, context.sortNullFirsts, context.preSortedChannelPrefix,
                context.expectedPositions, context.argumentChannels,
                VecTypeSerializer.serialize(context.windowFunctionReturnType));
        }
    }
}
