/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.partitionedoutput;

import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * The type Omni partitionedoutput operator factory.
 *
 * @since 20210630
 */
public class OmniPartitionedOutPutOperatorFactory
        extends OmniOperatorFactory<OmniPartitionedOutPutOperatorFactory.Context> {
    public OmniPartitionedOutPutOperatorFactory(VecType[] sourceTypes,
                                                boolean replicatesAnyRow,
                                                OptionalInt nullChannel,
                                                int[] partitionChannels,
                                                int partitionCount,
                                                int[] bucketToPartition,
                                                boolean isHashPrecomputed,
                                                VecType[] hashChannelTypes,
                                                int[] hashChannels) {
        super(new Context(sourceTypes, replicatesAnyRow, nullChannel, partitionChannels, partitionCount,
                bucketToPartition, isHashPrecomputed, hashChannelTypes, hashChannels));
    }

    @Override
    protected long createNativeOperatorFactory(Context context) {
        int nullChannel = context.nullChannel.isPresent() ? context.nullChannel.getAsInt() : -1;
        return createPartitionedOutputOperatorFactory(
                VecTypeSerializer.serialize(context.sourceTypes),
                context.replicatesAnyRow, nullChannel,
                context.partitionChannels, context.partitionCount,
                context.bucketToPartition, context.isHashPrecomputed,
                VecTypeSerializer.serialize(context.hashChannelTypes),
                context.hashChannels);
    }

    private static native long createPartitionedOutputOperatorFactory(
            String sourceTypes, boolean replicatesAnyRow,
            int nullChannel, int[] partitionChannels,
            int partitionCount, int[] bucketToPartition,
            boolean isHashPrecomputed, String hashChannelTypes,
            int[] hashChannels);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context
            extends OmniOperatorFactoryContext {
        private final VecType[] sourceTypes;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final int[] partitionChannels;
        private final int partitionCount;
        private final int[] bucketToPartition;
        private final boolean isHashPrecomputed;
        private final VecType[] hashChannelTypes;
        private final int[] hashChannels;

        public Context(VecType[] sourceTypes, boolean replicatesAnyRow, OptionalInt nullChannel,
                int[] partitionChannels, int partitionCount, int[] bucketToPartition, boolean isHashPrecomputed,
                VecType[] hashChannelTypes, int[] hashChannels) {
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
            Context context = null;
            if (obj instanceof Context) {
                context = (Context) obj;
            }
            return replicatesAnyRow == context.replicatesAnyRow
                    && partitionCount == context.partitionCount
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
}
