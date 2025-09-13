/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package nova.hetu.omniruntime.operator.topnsort;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type omni top n sort with expression operator factory.
 *
 * @since 2023-5-26
 */
public class OmniTopNSortWithExprOperatorFactory
        extends OmniOperatorFactory<OmniTopNSortWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new omni top n sort with expression operator factory.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param isStrictTopN true for window RowNumber, false for window Rank
     * @param partitionKeys the partition keys
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     * @param operatorConfig the operator config
     */
    public OmniTopNSortWithExprOperatorFactory(DataType[] sourceTypes, int limitN, boolean isStrictTopN,
            String[] partitionKeys, String[] sortKeys, int[] sortAssendings, int[] sortNullFirsts,
            OperatorConfig operatorConfig) {
        super(new FactoryContext(sourceTypes, limitN, isStrictTopN, partitionKeys, sortKeys, sortAssendings,
                sortNullFirsts, operatorConfig));
    }

    /**
     * Instantiates a new omni top n sort with expression operator factory with default
     * operator config.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param isStrictTopN true for window RowNumber, false for window Rank
     * @param partitionKeys the partition keys
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniTopNSortWithExprOperatorFactory(DataType[] sourceTypes, int limitN, boolean isStrictTopN,
            String[] partitionKeys, String[] sortKeys, int[] sortAssendings, int[] sortNullFirsts) {
        this(sourceTypes, limitN, isStrictTopN, partitionKeys, sortKeys, sortAssendings, sortNullFirsts,
                new OperatorConfig());
    }

    private static native long createTopNSortWithExprOperatorFactory(String sourceTypes, int limitN,
            boolean isStrictTopN, String[] partitionKeys, String[] sortKeys, int[] sortAssendings, int[] sortNullFirsts,
            String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createTopNSortWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
                context.isStrictTopN, context.partitionKeys, context.sortKeys, context.sortAssendings,
                context.sortNullFirsts, OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * The type Context.
     *
     * @since 2023-5-26
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final DataType[] sourceTypes;

        private final int limitN;

        private final boolean isStrictTopN;

        private final String[] partitionKeys;

        private final String[] sortKeys;

        private final int[] sortAssendings;

        private final int[] sortNullFirsts;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param limitN the limit n
         * @param isStrictTopN true for window RowNumber, false for window Rank
         * @param partitionKeys the partition keys
         * @param sortKeys the sort keys
         * @param sortAssendings the sort assendings
         * @param sortNullFirsts the sort null firsts
         * @param operatorConfig the operator config
         */
        public FactoryContext(DataType[] sourceTypes, int limitN, boolean isStrictTopN, String[] partitionKeys,
                String[] sortKeys, int[] sortAssendings, int[] sortNullFirsts, OperatorConfig operatorConfig) {
            this.sourceTypes = sourceTypes;
            this.limitN = limitN;
            this.isStrictTopN = isStrictTopN;
            this.partitionKeys = partitionKeys;
            this.sortKeys = sortKeys;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
            this.operatorConfig = operatorConfig;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), limitN, isStrictTopN, Arrays.hashCode(partitionKeys),
                    Arrays.hashCode(sortKeys), Arrays.hashCode(sortAssendings), Arrays.hashCode(sortNullFirsts),
                    operatorConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FactoryContext context = (FactoryContext) obj;
            return limitN == context.limitN && Arrays.equals(sourceTypes, context.sourceTypes)
                    && isStrictTopN == context.isStrictTopN && Arrays.equals(partitionKeys, context.partitionKeys)
                    && Arrays.equals(sortKeys, context.sortKeys)
                    && Arrays.equals(sortAssendings, context.sortAssendings)
                    && Arrays.equals(sortNullFirsts, context.sortNullFirsts)
                    && operatorConfig.equals(context.operatorConfig);
        }
    }
}
