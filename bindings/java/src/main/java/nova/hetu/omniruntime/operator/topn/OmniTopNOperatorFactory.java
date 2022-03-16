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
 * The type Omni top n operator factory.
 *
 * @since 20210630
 */
public class OmniTopNOperatorFactory extends OmniOperatorFactory<OmniTopNOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni top n operator factory.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param sortCols the sort cols
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniTopNOperatorFactory(DataType[] sourceTypes, int limitN, String[] sortCols, int[] sortAssendings,
        int[] sortNullFirsts) {
        super(new FactoryContext(new JitContext(sourceTypes, limitN, sortCols, sortAssendings, sortNullFirsts)));
    }

    private static native long createTopNOperatorFactory(String sourceTypes, int limitN, String[] sortCols,
        int[] sortAssendings, int[] sortNullFirsts, long nativeJitContext);

    private static native long createTopNJitContext(String sourceTypes, int limitN, String[] sortCols,
        int[] sortAssendings, int[] sortNullFirsts);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createTopNOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
            context.sortCols, context.sortAssendings, context.sortNullFirsts, factoryContext.getNativeJitContext());
    }

    /**
     * The type Jit context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final int limitN;

        private final String[] sortCols;

        private final int[] sortAssendings;

        private final int[] sortNullFirsts;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param limitN the limit n
         * @param sortCols the sort cols
         * @param sortAssendings the sort assendings
         * @param sortNullFirsts the sort null firsts
         */
        public JitContext(DataType[] sourceTypes, int limitN, String[] sortCols, int[] sortAssendings,
            int[] sortNullFirsts) {
            this.sourceTypes = sourceTypes;
            this.limitN = limitN;
            this.sortCols = sortCols;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), limitN, Arrays.hashCode(sortCols),
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
            return limitN == context.limitN && Arrays.equals(sourceTypes, context.sourceTypes) && Arrays.equals(
                sortCols, context.sortCols) && Arrays.equals(sortAssendings, context.sortAssendings) && Arrays.equals(
                sortNullFirsts, context.sortNullFirsts);
        }
    }

    /**
     * The type Context.
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
            return createTopNJitContext(DataTypeSerializer.serialize(context.sourceTypes), context.limitN,
                context.sortCols, context.sortAssendings, context.sortNullFirsts);
        }
    }
}
