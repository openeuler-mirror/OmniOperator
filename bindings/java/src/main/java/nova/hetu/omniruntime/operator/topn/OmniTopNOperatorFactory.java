/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.topn;

import static nova.hetu.omniruntime.constants.ConstantHelper.toNativeConstants;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni top n operator factory.
 *
 * @since 20210630
 */
public class OmniTopNOperatorFactory extends OmniOperatorFactory<OmniTopNOperatorFactory.Context> {
    /**
     * Instantiates a new Omni top n operator factory.
     *
     * @param sourceTypes the source types
     * @param limitN the limit n
     * @param sortCols the sort cols
     * @param sortAssendings the sort assendings
     * @param sortNullFirsts the sort null firsts
     */
    public OmniTopNOperatorFactory(VecType[] sourceTypes, int limitN, int[] sortCols, int[] sortAssendings,
        int[] sortNullFirsts) {
        super(new Context(sourceTypes, limitN, sortCols, sortAssendings, sortNullFirsts));
    }

    private static native long createTopNOperatorFactory(int[] sourceTypes, int limitN, int[] sortCols, int[] sortAssendings,
        int[] sortNullFirsts);

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createTopNOperatorFactory(toNativeConstants(context.sourceTypes), context.limitN, context.sortCols,
            context.sortAssendings, context.sortNullFirsts);
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] sourceTypes;

        private final int limitN;

        private final int[] sortCols;

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
        public Context(VecType[] sourceTypes, int limitN, int[] sortCols, int[] sortAssendings, int[] sortNullFirsts) {
            this.sourceTypes = sourceTypes;
            this.limitN = limitN;
            this.sortCols = sortCols;
            this.sortAssendings = sortAssendings;
            this.sortNullFirsts = sortNullFirsts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceTypes, limitN, sortCols, sortAssendings, sortNullFirsts);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            if (!super.equals(obj)) {
                return false;
            }
            Context context = (Context) obj;
            return limitN == context.limitN && Arrays.equals(sourceTypes, context.sourceTypes) && Arrays.equals(sortCols,
                context.sortCols) && Arrays.equals(sortAssendings, context.sortAssendings) && Arrays.equals(
                sortNullFirsts, context.sortNullFirsts);
        }
    }
}
