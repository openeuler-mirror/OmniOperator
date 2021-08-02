/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

package nova.hetu.omniruntime.operator.partitioned;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

/**
 * The type Omni PartitionedOutPut operator factory.
 *
 * @since 20210630
 */
public class OmniPartitionedOutPutOperatorFactory
        extends OmniOperatorFactory<OmniPartitionedOutPutOperatorFactory.Context> {
    public OmniPartitionedOutPutOperatorFactory(VecType[] sourceTypes, boolean replicatesAnyRow,
                                                OptionalInt nullChannel, int[] partitionChannels, int partitionCount,
                                                int[] bucketToPartition) {
        super(new Context(sourceTypes, replicatesAnyRow, nullChannel, partitionChannels, partitionCount,
                bucketToPartition));
    }

    @Override
    protected long createNativeOperatorFactory(Context context) {
        int nullChannel = context.nullChannel.isPresent() ? context.nullChannel.getAsInt() : -1;
        return createPartitionedOperatorFactory(
            toNativeConstants(context.sourceTypes), context.replicatesAnyRow, nullChannel,
            context.partitionChannels, context.partitionCount, context.bucketToPartition);
    }

    private static native long createPartitionedOperatorFactory(int[] sourceTypes, boolean replicatesAnyRow,
        int nullChannel, int[] partitionChannels,
        int partitionCount, int[] bucketToPartition);

    /**
     * The type Context.
     */
    public static class Context
            extends OmniOperatorFactoryContext {
        private final VecType[] sourceTypes;

        private final boolean replicatesAnyRow;

        private final OptionalInt nullChannel;

        private final int[] partitionChannels;

        private final int partitionCount;

        private final int[] bucketToPartition;

        public Context(VecType[] sourceTypes, boolean replicatesAnyRow, OptionalInt nullChannel,
            int[] partitionChannels, int partitionCount, int[] bucketToPartition) {
            this.sourceTypes = sourceTypes;
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = nullChannel;
            this.partitionChannels = partitionChannels;
            this.partitionCount = partitionCount;
            this.bucketToPartition = bucketToPartition;
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
            if (obj instanceof  Context) {
                context = (Context) obj;
            }
            return replicatesAnyRow == context.replicatesAnyRow && partitionCount == context.partitionCount
                && Arrays.equals(sourceTypes, context.sourceTypes) && Objects.equals(nullChannel, context.nullChannel)
                && Arrays.equals(partitionChannels, context.partitionChannels) && Arrays.equals(bucketToPartition,
                context.bucketToPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), replicatesAnyRow, nullChannel, partitionCount,
                Arrays.hashCode(bucketToPartition));
        }
    }
}
