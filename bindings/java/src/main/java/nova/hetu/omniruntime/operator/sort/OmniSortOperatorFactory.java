/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.sort;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni sort operator factory.
 *
 * @since 20210630
 */
public class OmniSortOperatorFactory extends OmniOperatorFactory<OmniSortOperatorFactory.Context> {
    /**
     * Instantiates a new Omni sort operator factory.
     *
     * @param sourceTypes the source types
     * @param outputColumns the output columns
     * @param sortColumns the sort columns
     * @param sortAscendings the sort ascendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniSortOperatorFactory(VecType[] sourceTypes, int[] outputColumns, int[] sortColumns, int[] sortAscendings,
        int[] sortNullFirsts) {
        super(new Context(sourceTypes, outputColumns, sortColumns, sortAscendings, sortNullFirsts));
    }

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createSortOperatorFactory(toNativeConstants(context.sourceTypes), context.outputColumns,
            context.sortColumns, context.sortAscendings, context.sortNullFirsts);
    }

    private static native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols,
        int[] ascendings, int[] nullFirsts);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] sourceTypes;

        private final int[] outputColumns;

        private final int[] sortColumns;

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
        public Context(VecType[] sourceTypes, int[] outputColumns, int[] sortColumns, int[] sortAscendings,
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
            Context that = (Context) obj;
            return Arrays.equals(sourceTypes, that.sourceTypes) && Arrays.equals(outputColumns, that.outputColumns)
                && Arrays.equals(sortColumns, that.sortColumns) && Arrays.equals(sortAscendings, that.sortAscendings)
                && Arrays.equals(sortNullFirsts, that.sortNullFirsts);
        }
    }
}
