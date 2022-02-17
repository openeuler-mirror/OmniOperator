/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.sort;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The Omni sort with expression operator factory.
 */
public class OmniSortWithExprOperatorFactory
        extends
            OmniOperatorFactory<OmniSortWithExprOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni sort with expression operator factory.
     *
     * @param sourceTypes the source types
     * @param outputColumns the output columns
     * @param sortKeys the sort keys
     * @param sortAscendings the sort ascendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniSortWithExprOperatorFactory(DataType[] sourceTypes, int[] outputColumns, String[] sortKeys,
            int[] sortAscendings, int[] sortNullFirsts) {
        super(new FactoryContext(new JitContext(sourceTypes, outputColumns, sortKeys, sortAscendings, sortNullFirsts)));
    }

    private static native long createSortWithExprOperatorFactory(String sourceTypes, int[] outputCols,
            String[] sortKeys, int[] ascendings, int[] nullFirsts, long jitContext);

    private static native long createSortWithExprJitContext(String sourceTypes, int[] outputCols, String[] sortKeys,
            int[] ascendings, int[] nullFirsts);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createSortWithExprOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.outputColumns, context.sortKeys, context.sortAscendings, context.sortNullFirsts,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final int[] outputColumns;

        private final String[] sortKeys;

        private final int[] sortAscendings;

        private final int[] sortNullFirsts;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param outputColumns the output columns
         * @param sortKeys the sort keys
         * @param sortAscendings the sort ascendings
         * @param sortNullFirsts the sort null firsts
         */
        public JitContext(DataType[] sourceTypes, int[] outputColumns, String[] sortKeys, int[] sortAscendings,
                int[] sortNullFirsts) {
            this.sourceTypes = sourceTypes;
            this.outputColumns = outputColumns;
            this.sortKeys = sortKeys;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputColumns), Arrays.hashCode(sortKeys),
                    Arrays.hashCode(sortAscendings), Arrays.hashCode(sortNullFirsts));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JitContext that = (JitContext) obj;
            return Arrays.equals(sourceTypes, that.sourceTypes) && Arrays.equals(outputColumns, that.outputColumns)
                    && Arrays.equals(sortKeys, that.sortKeys) && Arrays.equals(sortAscendings, that.sortAscendings)
                    && Arrays.equals(sortNullFirsts, that.sortNullFirsts);
        }
    }

    /**
     * The Factory context.
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
            return createSortWithExprJitContext(DataTypeSerializer.serialize(context.sourceTypes), context.outputColumns,
                    context.sortKeys, context.sortAscendings, context.sortNullFirsts);
        }
    }
}
