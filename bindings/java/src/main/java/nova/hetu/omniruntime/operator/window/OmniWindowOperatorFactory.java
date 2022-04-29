/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.window;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

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
     * @param operatorConfig the operator config
     */
    public OmniWindowOperatorFactory(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            DataType[] windowFunctionReturnType, OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(sourceTypes, outputChannels, windowFunction, partitionChannels,
                preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions,
                argumentChannels, windowFunctionReturnType, operatorConfig)));
    }

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
     * @param windowFrameTypes frame types of the window
     * @param windowFrameStartTypes start types of frame in window
     * @param winddowFrameStartChannels channels value of frame start value
     * @param windowFrameEndTypes end types of frame in window
     * @param winddowFrameEndChannels channel values of frame end value
     * @param operatorConfig the operator config
     */
    public OmniWindowOperatorFactory(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            DataType[] windowFunctionReturnType, OmniWindowFrameType[] windowFrameTypes,
            OmniWindowFrameBoundType[] windowFrameStartTypes, int[] winddowFrameStartChannels,
            OmniWindowFrameBoundType[] windowFrameEndTypes, int[] winddowFrameEndChannels,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(new JitContext(sourceTypes, outputChannels, windowFunction, partitionChannels,
                preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions,
                argumentChannels, windowFunctionReturnType, windowFrameTypes, windowFrameStartTypes,
                winddowFrameStartChannels, windowFrameEndTypes, winddowFrameEndChannels, operatorConfig)));
    }

    /**
     * Instantiates a new Omni window operator factory with default operator config.
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
    public OmniWindowOperatorFactory(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            DataType[] windowFunctionReturnType) {
        this(sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels,
                sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels,
                windowFunctionReturnType, new OperatorConfig(true));
    }

    /**
     * Instantiates a new Omni window operator factory with default operator config.
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
     * @param windowFrameTypes frame types of the window
     * @param windowFrameStartTypes start types of frame in window
     * @param winddowFrameStartChannels channels value of frame start value
     * @param windowFrameEndTypes end types of frame in window
     * @param winddowFrameEndChannels channel values of frame end value
     */
    public OmniWindowOperatorFactory(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            DataType[] windowFunctionReturnType, OmniWindowFrameType[] windowFrameTypes,
            OmniWindowFrameBoundType[] windowFrameStartTypes, int[] winddowFrameStartChannels,
            OmniWindowFrameBoundType[] windowFrameEndTypes, int[] winddowFrameEndChannels) {
        this(sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels,
                sortOrder, sortNullFirsts, preSortedChannelPrefix, expectedPositions, argumentChannels,
                windowFunctionReturnType, windowFrameTypes, windowFrameStartTypes, winddowFrameStartChannels,
                windowFrameEndTypes, winddowFrameEndChannels, new OperatorConfig(true));
    }

    private static native long createWindowJitContext(String sourceTypes, int[] outputChannels, int[] windFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            String windowFunctionReturnType, int[] windowFrameTypes, int[] windowFrameStartTypes,
            int[] windowFrameStartChannels, int[] windowFrameEndTypes, int[] windowFrameEndChannels);

    private static native long createWindowOperatorFactory(String sourceTypes, int[] outputChannels, int[] windFunction,
            int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
            int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
            String windowFunctionReturnType, int[] windowFrameTypes, int[] windowFrameStartTypes,
            int[] windowFrameStartChannels, int[] windowFrameEndTypes, int[] windowFrameEndChannels, long jitContext);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createWindowOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.outputChannels,
                toNativeConstants(context.windFunction), context.partitionChannels, context.preGroupedChannels,
                context.sortChannels, context.sortOrder, context.sortNullFirsts, context.preSortedChannelPrefix,
                context.expectedPositions, context.argumentChannels,
                DataTypeSerializer.serialize(context.windowFunctionReturnType), toNativeConstants(context.frameTypes),
                toNativeConstants(context.frameStartTypes), context.frameStartChannels,
                toNativeConstants(context.frameEndTypes), context.frameEndChannels,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext extends OmniJitContext {
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

        private final int[] argumentChannels;

        private final DataType[] windowFunctionReturnType;

        private final OmniWindowFrameType[] frameTypes;

        private final OmniWindowFrameBoundType[] frameStartTypes;

        private final int[] frameStartChannels;

        private final OmniWindowFrameBoundType[] frameEndTypes;

        private final int[] frameEndChannels;

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
         * @param operatorConfig the operator config
         */
        public JitContext(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
                int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
                int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
                DataType[] windowFunctionReturnType, OperatorConfig operatorConfig) {
            super(operatorConfig);
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

            /*
             * these code and constructor needs to be deleted when all engine adapter
             * finished
             */
            this.frameTypes = new OmniWindowFrameType[windowFunction.length];
            this.frameStartTypes = new OmniWindowFrameBoundType[windowFunction.length];
            this.frameStartChannels = new int[windowFunction.length];
            this.frameEndTypes = new OmniWindowFrameBoundType[windowFunction.length];
            this.frameEndChannels = new int[windowFunction.length];
            for (int i = 0; i < windowFunction.length; i++) {
                this.frameTypes[i] = OmniWindowFrameType.OMNI_FRAME_TYPE_RANGE;
                this.frameStartTypes[i] = OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING;
                this.frameStartChannels[i] = -1;
                this.frameEndTypes[i] = OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW;
                this.frameEndChannels[i] = -1;
            }
        }

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
         * @param windowFrameTypes frame types of the window
         * @param windowFrameStartTypes start types of frame in window
         * @param winddowFrameStartChannels channels value of frame start value
         * @param windowFrameEndTypes end types of frame in window
         * @param winddowFrameEndChannels channel values of frame end value
         * @param operatorConfig the operator config
         */
        public JitContext(DataType[] sourceTypes, int[] outputChannels, FunctionType[] windowFunction,
                int[] partitionChannels, int[] preGroupedChannels, int[] sortChannels, int[] sortOrder,
                int[] sortNullFirsts, int preSortedChannelPrefix, int expectedPositions, int[] argumentChannels,
                DataType[] windowFunctionReturnType, OmniWindowFrameType[] windowFrameTypes,
                OmniWindowFrameBoundType[] windowFrameStartTypes, int[] winddowFrameStartChannels,
                OmniWindowFrameBoundType[] windowFrameEndTypes, int[] winddowFrameEndChannels,
                OperatorConfig operatorConfig) {
            super(operatorConfig);
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
            this.frameTypes = windowFrameTypes;
            this.frameStartTypes = windowFrameStartTypes;
            this.frameStartChannels = winddowFrameStartChannels;
            this.frameEndTypes = windowFrameEndTypes;
            this.frameEndChannels = winddowFrameEndChannels;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputChannels),
                    Arrays.hashCode(windFunction), Arrays.hashCode(partitionChannels),
                    Arrays.hashCode(preGroupedChannels), Arrays.hashCode(sortChannels), Arrays.hashCode(sortOrder),
                    Arrays.hashCode(sortNullFirsts), preSortedChannelPrefix, expectedPositions,
                    Arrays.hashCode(argumentChannels), Arrays.hashCode(windowFunctionReturnType),
                    Arrays.hashCode(frameTypes), Arrays.hashCode(frameStartTypes), Arrays.hashCode(frameStartChannels),
                    Arrays.hashCode(frameEndTypes), Arrays.hashCode(frameEndChannels), operatorConfig);
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
                    && Arrays.equals(argumentChannels, context.argumentChannels)
                    && Arrays.equals(windowFunctionReturnType, context.windowFunctionReturnType)
                    && Arrays.equals(frameTypes, context.frameTypes)
                    && Arrays.equals(frameStartTypes, context.frameStartTypes)
                    && Arrays.equals(frameStartChannels, context.frameStartChannels)
                    && Arrays.equals(frameEndTypes, context.frameEndTypes)
                    && Arrays.equals(frameEndChannels, context.frameEndChannels)
                    && operatorConfig.equals(context.operatorConfig);
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
            return createWindowJitContext(DataTypeSerializer.serialize(context.sourceTypes), context.outputChannels,
                    toNativeConstants(context.windFunction), context.partitionChannels, context.preGroupedChannels,
                    context.sortChannels, context.sortOrder, context.sortNullFirsts, context.preSortedChannelPrefix,
                    context.expectedPositions, context.argumentChannels,
                    DataTypeSerializer.serialize(context.windowFunctionReturnType),
                    toNativeConstants(context.frameTypes), toNativeConstants(context.frameStartTypes),
                    context.frameStartChannels, toNativeConstants(context.frameEndTypes), context.frameEndChannels);
        }
    }
}
