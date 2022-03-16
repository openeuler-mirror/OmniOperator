/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.partitionedoutput;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * The type Omni partitionedoutput operator factory.
 *
 * @since 20210630
 */
public class OmniPartitionedOutPutOperatorFactory
        extends
            OmniOperatorFactory<OmniPartitionedOutPutOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni partitioned out put operator factory.
     *
     * @param sourceTypes the source types
     * @param replicatesAnyRow the replicates any row
     * @param nullChannel the null channel
     * @param partitionChannels the partition channels
     * @param partitionCount the partition count
     * @param bucketToPartition the bucket to partition
     * @param isHashPrecomputed the is hash precomputed
     * @param hashChannelTypes the hash channel types
     * @param hashChannels the hash channels
     */
    public OmniPartitionedOutPutOperatorFactory(DataType[] sourceTypes, boolean replicatesAnyRow,
            OptionalInt nullChannel, int[] partitionChannels, int partitionCount, int[] bucketToPartition,
            boolean isHashPrecomputed, DataType[] hashChannelTypes, int[] hashChannels) {
        super(new FactoryContext(new JitContext(sourceTypes, replicatesAnyRow, nullChannel, partitionChannels,
                partitionCount, bucketToPartition, isHashPrecomputed, hashChannelTypes, hashChannels)));
    }

    private static native long createPartitionedOutputOperatorFactory(String sourceTypes, boolean replicatesAnyRow,
            int nullChannel, int[] partitionChannels, int partitionCount, int[] bucketToPartition,
            boolean isHashPrecomputed, String hashChannelTypes, int[] hashChannels, long jitContext);

    private static native long createPartitionedOutputJitContext(String sourceTypes, boolean replicatesAnyRow,
            int nullChannel, int[] partitionChannels, int partitionCount, int[] bucketToPartition,
            boolean isHashPrecomputed, String hashChannelTypes, int[] hashChannels);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        int nullChannel = context.nullChannel.isPresent() ? context.nullChannel.getAsInt() : -1;
        return createPartitionedOutputOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.replicatesAnyRow, nullChannel, context.partitionChannels, context.partitionCount,
                context.bucketToPartition, context.isHashPrecomputed,
                DataTypeSerializer.serialize(context.hashChannelTypes), context.hashChannels,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final boolean replicatesAnyRow;

        private final OptionalInt nullChannel;

        private final int[] partitionChannels;

        private final int partitionCount;

        private final int[] bucketToPartition;

        private final boolean isHashPrecomputed;

        private final DataType[] hashChannelTypes;

        private final int[] hashChannels;

        /**
         * Instantiates a new Jit context.
         *
         * @param sourceTypes the source types
         * @param replicatesAnyRow the replicates any row
         * @param nullChannel the null channel
         * @param partitionChannels the partition channels
         * @param partitionCount the partition count
         * @param bucketToPartition the bucket to partition
         * @param isHashPrecomputed the is hash precomputed
         * @param hashChannelTypes the hash channel types
         * @param hashChannels the hash channels
         */
        public JitContext(DataType[] sourceTypes, boolean replicatesAnyRow, OptionalInt nullChannel,
                int[] partitionChannels, int partitionCount, int[] bucketToPartition, boolean isHashPrecomputed,
                DataType[] hashChannelTypes, int[] hashChannels) {
            this.sourceTypes = sourceTypes;
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = nullChannel;
            this.partitionChannels = partitionChannels;
            this.partitionCount = partitionCount;
            this.bucketToPartition = bucketToPartition;
            this.isHashPrecomputed = isHashPrecomputed;
            this.hashChannelTypes = hashChannelTypes;
            this.hashChannels = hashChannels;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext context = null;
            if (obj instanceof JitContext) {
                context = (JitContext) obj;
            }
            return replicatesAnyRow == context.replicatesAnyRow && partitionCount == context.partitionCount
                    && Arrays.equals(sourceTypes, context.sourceTypes)
                    && Objects.equals(nullChannel, context.nullChannel)
                    && Arrays.equals(partitionChannels, context.partitionChannels)
                    && Arrays.equals(bucketToPartition, context.bucketToPartition)
                    && context.isHashPrecomputed == isHashPrecomputed
                    && Arrays.equals(hashChannelTypes, context.hashChannelTypes)
                    && Arrays.equals(hashChannels, context.hashChannels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), replicatesAnyRow, nullChannel,
                    Arrays.hashCode(partitionChannels), partitionCount, Arrays.hashCode(bucketToPartition),
                    isHashPrecomputed, Arrays.hashCode(hashChannelTypes), Arrays.hashCode(hashChannels));
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
            // todo: use createPartitionedOutputJitContext when there is a jit optimization
            return 0;
        }
    }
}
