/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.topn;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
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
     */
    public OmniTopNWithExprOperatorFactory(DataType[] sourceTypes, int limitN, String[] sortKeys, int[] sortAssendings,
            int[] sortNullFirsts) {
        super(new FactoryContext(new JitContext(sourceTypes, limitN, sortKeys, sortAssendings, sortNullFirsts)));
    }

    private static native long createTopNWithExprOperatorFactory(String sourceTypes, int limitN, String[] sortKeys,
            int[] sortAssendings, int[] sortNullFirsts, long nativeJitContext);

    private static native long createTopNWithExprJitContext(String sourceTypes, int limitN, String[] sortKeys,
            int[] sortAssendings, int[] sortNullFirsts);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createTopNWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
                context.sortKeys, context.sortAssendings, context.sortNullFirsts, factoryContext.getNativeJitContext());
    }

    /**
     * The type Jit context.
     *
     * @since 2021-06-30
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final int limitN;

        private final String[] sortKeys;

        private final int[] sortAssendings;

        private final int[] sortNullFirsts;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param limitN the limit n
         * @param sortKeys the sort cols
         * @param sortAssendings the sort assendings
         * @param sortNullFirsts the sort null firsts
         */
        public JitContext(DataType[] sourceTypes, int limitN, String[] sortKeys, int[] sortAssendings,
                int[] sortNullFirsts) {
            this.sourceTypes = sourceTypes;
            this.limitN = limitN;
            this.sortKeys = sortKeys;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), limitN, Arrays.hashCode(sortKeys),
                    Arrays.hashCode(sortAssendings), Arrays.hashCode(sortNullFirsts));
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
            return limitN == context.limitN && Arrays.equals(sourceTypes, context.sourceTypes)
                    && Arrays.equals(sortKeys, context.sortKeys)
                    && Arrays.equals(sortAssendings, context.sortAssendings)
                    && Arrays.equals(sortNullFirsts, context.sortNullFirsts);
        }
    }

    /**
     * The type Context.
     *
     * @since 2021-10-26
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
            return createTopNWithExprJitContext(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
                    context.sortKeys, context.sortAssendings, context.sortNullFirsts);
        }
    }
}
