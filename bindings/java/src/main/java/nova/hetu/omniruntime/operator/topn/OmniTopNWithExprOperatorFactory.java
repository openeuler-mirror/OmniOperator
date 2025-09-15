/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.topn;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni top n with expression operator factory.
 *
 * @since 2021-10-26
 */
public class OmniTopNWithExprOperatorFactory
        extends OmniOperatorFactory<OmniTopNWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni top n with expression operator factory.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     * @param operatorConfig the operator config
     */
    public OmniTopNWithExprOperatorFactory(DataType[] sourceTypes, int limitN, String[] sortKeys, int[] sortAssendings,
            int[] sortNullFirsts, OperatorConfig operatorConfig) {
        super(new FactoryContext(sourceTypes, limitN, 0, sortKeys, sortAssendings, sortNullFirsts, operatorConfig));
    }

    /**
     * Instantiates a new Omni top n with expression operator factory.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param offsetN the offset n
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     * @param operatorConfig the operator config
     */
    public OmniTopNWithExprOperatorFactory(DataType[] sourceTypes, int limitN, int offsetN, String[] sortKeys,
            int[] sortAssendings, int[] sortNullFirsts, OperatorConfig operatorConfig) {
        super(new FactoryContext(sourceTypes, limitN, offsetN, sortKeys, sortAssendings, sortNullFirsts,
                operatorConfig));
    }

    /**
     * Instantiates a new Omni top n with expression operator factory with default
     * operator config.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniTopNWithExprOperatorFactory(DataType[] sourceTypes, int limitN, String[] sortKeys, int[] sortAssendings,
            int[] sortNullFirsts) {
        this(sourceTypes, limitN, 0, sortKeys, sortAssendings, sortNullFirsts, new OperatorConfig());
    }

    /**
     * Instantiates a new Omni top n with expression operator factory with default
     * operator config.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param offsetN the offset n
     * @param sortKeys the sort keys
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniTopNWithExprOperatorFactory(DataType[] sourceTypes, int limitN, int offsetN, String[] sortKeys,
            int[] sortAssendings, int[] sortNullFirsts) {
        this(sourceTypes, limitN, offsetN, sortKeys, sortAssendings, sortNullFirsts, new OperatorConfig());
    }

    private static native long createTopNWithExprOperatorFactory(String sourceTypes, int limitN, int offsetN,
            String[] sortKeys, int[] sortAssendings, int[] sortNullFirsts, String operatorConfig);

    @Override
    protected long createNativeOperatorFactory(FactoryContext context) {
        return createTopNWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
                context.offsetN, context.sortKeys, context.sortAssendings, context.sortNullFirsts,
                OperatorConfig.serialize(context.operatorConfig));
    }

    /**
     * The type Context.
     *
     * @since 2021-10-26
     */
    public static class FactoryContext extends OmniOperatorFactoryContext {
        private final DataType[] sourceTypes;

        private final int limitN;

        private final int offsetN;

        private final String[] sortKeys;

        private final int[] sortAssendings;

        private final int[] sortNullFirsts;

        private final OperatorConfig operatorConfig;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param limitN the limit n
         * @param offsetN the offset n
         * @param sortKeys the sort cols
         * @param sortAssendings the sort assendings
         * @param sortNullFirsts the sort null firsts
         * @param operatorConfig the operator config
         */
        public FactoryContext(DataType[] sourceTypes, int limitN, int offsetN, String[] sortKeys, int[] sortAssendings,
                int[] sortNullFirsts, OperatorConfig operatorConfig) {
            this.sourceTypes = sourceTypes;
            this.limitN = limitN;
            this.offsetN = offsetN;
            this.sortKeys = sortKeys;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
            this.operatorConfig = operatorConfig;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), limitN, offsetN, Arrays.hashCode(sortKeys),
                    Arrays.hashCode(sortAssendings), Arrays.hashCode(sortNullFirsts), operatorConfig);
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
            return limitN == context.limitN && offsetN == context.offsetN
                    && Arrays.equals(sourceTypes, context.sourceTypes) && Arrays.equals(sortKeys, context.sortKeys)
                    && Arrays.equals(sortAssendings, context.sortAssendings)
                    && Arrays.equals(sortNullFirsts, context.sortNullFirsts)
                    && operatorConfig.equals(context.operatorConfig);
        }
    }
}
