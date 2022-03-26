/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
 * The type Omni sort operator factory.
 *
 * @since 2021-06-30
 */
public class OmniSortOperatorFactory extends OmniOperatorFactory<OmniSortOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni sort operator factory.
     *
     * @param sourceTypes the source types
     * @param outputColumns the output columns
     * @param sortColumns the sort columns
     * @param sortAscendings the sort ascendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniSortOperatorFactory(DataType[] sourceTypes, int[] outputColumns, String[] sortColumns,
            int[] sortAscendings, int[] sortNullFirsts) {
        super(new FactoryContext(
                new JitContext(sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts)));
    }

    private static native long createSortOperatorFactory(String sourceTypes, int[] outputCols, String[] sortCols,
            int[] ascendings, int[] nullFirsts, long jitContext);

    private static native long createSortJitContext(String sourceTypes, int[] outputCols, String[] sortCols,
            int[] ascendings, int[] nullFirsts);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createSortOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes), context.outputColumns,
                context.sortColumns, context.sortAscendings, context.sortNullFirsts,
                factoryContext.getNativeJitContext());
    }

    /**
     * The type Context.
     *
     * @since 2021-06-30
     */
    public static class JitContext implements OmniJitContext {
        private final DataType[] sourceTypes;

        private final int[] outputColumns;

        private final String[] sortColumns;

        private final int[] sortAscendings;

        private final int[] sortNullFirsts;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the source types
         * @param outputColumns the output columns
         * @param sortColumns the sort columns
         * @param sortAscendings the sort ascendings
         * @param sortNullFirsts the sort null firsts
         */
        public JitContext(DataType[] sourceTypes, int[] outputColumns, String[] sortColumns, int[] sortAscendings,
                int[] sortNullFirsts) {
            this.sourceTypes = sourceTypes;
            this.outputColumns = outputColumns;
            this.sortColumns = sortColumns;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(outputColumns),
                    Arrays.hashCode(sortColumns), Arrays.hashCode(sortAscendings), Arrays.hashCode(sortNullFirsts));
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
                    && Arrays.equals(sortColumns, that.sortColumns)
                    && Arrays.equals(sortAscendings, that.sortAscendings)
                    && Arrays.equals(sortNullFirsts, that.sortNullFirsts);
        }
    }

    /**
     * The type Factory context.
     *
     * @since 2021-06-30
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
            return createSortJitContext(DataTypeSerializer.serialize(context.sourceTypes), context.outputColumns,
                    context.sortColumns, context.sortAscendings, context.sortNullFirsts);
        }
    }
}
