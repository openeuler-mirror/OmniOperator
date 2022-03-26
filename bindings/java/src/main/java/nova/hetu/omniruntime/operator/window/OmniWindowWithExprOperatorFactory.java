/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.window;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni window operator factory.
 *
 * @since 20210630
 */
public class OmniWindowWithExprOperatorFactory
        extends OmniOperatorFactory<OmniWindowWithExprOperatorFactory.FactoryContext> {
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
     * @param argumentKeys the argument keys
     * @param windowFunctionReturnType the window function return type
     */
    public OmniWindowWithExprOperatorFactory(DataType[] sourceTypes, int[] outputChannels,
            FunctionType[] windowFunction, int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels,
            int[] sortOrder, int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions,
            String[] argumentKeys, DataType[] windowFunctionReturnType) {
        super(new FactoryContext(new JitContext(sourceTypes, outputChannels, windowFunction, partitionChannels,
                preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions,
                argumentKeys, windowFunctionReturnType)));
    }

    private static native long createWindowWithExprJitContext(String sourceTypes, int[] outputChannels,
            int[] windFunction, int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, String[] argumentKeys,
            String windowFunctionReturnType);

    private static native long createWindowWithExprOperatorFactory(String sourceTypes, int[] outputChannels,
            int[] windFunction, int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, String[] argumentKeys,
            String windowFunctionReturnType, long jitContext);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createWindowWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.outputChannels, toNativeConstants(context.windFunction), context.partitionChannels,
                context.preGroupedChannels, context.sortChannels, context.sortOrder, context.sortNullFirsts,
                context.preSortedChannelPrefix, context.expectedPositions, context.argumentKeys,
                DataTypeSerializer.serialize(context.windowFunctionReturnType), factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final int[] outputChannels;

        private final FunctionType[] windFunction;

        private final int[] partitionChannels;

        private final int[] sortChannels;

        private final int[] sortOrder;

        private final int[] preGroupedChannels;

        private final int[] sortNullFirsts;

        private final int preSortedChannelPrefix;

        private final int expectedPositions;

        private final String[] argumentKeys;

        private final DataType[] windowFunctionReturnType;

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
         * @param argumentKeys the argument channels
         * @param windowFunctionReturnType the window function return type
         */
        public JitContext(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
                int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
                int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, String[] argumentKeys,
                DataType[] windowFunctionReturnType) {
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
            this.argumentKeys = argumentKeys;
            this.windowFunctionReturnType = windowFunctionReturnType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputChannels),
                    Arrays.hashCode(windFunction), Arrays.hashCode(partitionChannels),
                    Arrays.hashCode(preGroupedChannels), Arrays.hashCode(sortChannels), Arrays.hashCode(sortOrder),
                    Arrays.hashCode(sortNullFirsts), preSortedChannelPrefix, expectedPositions,
                    Arrays.hashCode(argumentKeys), Arrays.hashCode(windowFunctionReturnType));
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
                    && Arrays.equals(outputChannels, context.outputChannels)
                    && Arrays.equals(windFunction, context.windFunction)
                    && Arrays.equals(partitionChannels, context.partitionChannels)
                    && Arrays.equals(preGroupedChannels, context.preGroupedChannels)
                    && Arrays.equals(sortChannels, context.sortChannels) && Arrays.equals(sortOrder, context.sortOrder)
                    && Arrays.equals(sortNullFirsts, context.sortNullFirsts)
                    && Arrays.equals(argumentKeys, context.argumentKeys)
                    && Arrays.equals(windowFunctionReturnType, context.windowFunctionReturnType);
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
            return createWindowWithExprJitContext(DataTypeSerializer.serialize(context.sourceTypes),
                    context.outputChannels, toNativeConstants(context.windFunction), context.partitionChannels,
                    context.preGroupedChannels, context.sortChannels, context.sortOrder, context.sortNullFirsts,
                    context.preSortedChannelPrefix, context.expectedPositions, context.argumentKeys,
                    DataTypeSerializer.serialize(context.windowFunctionReturnType));
        }
    }
}
