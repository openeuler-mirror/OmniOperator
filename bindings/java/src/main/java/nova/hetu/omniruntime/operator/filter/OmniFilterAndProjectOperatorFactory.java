/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.filter;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The type Omni filter and project operator factory.
 *
 * @since 20210630
 */
public class OmniFilterAndProjectOperatorFactory
        extends OmniOperatorFactory<OmniFilterAndProjectOperatorFactory.Context> {
    /**
     * Instantiates a new Omni filter and project operator factory.
     *
     * @param expression     the expression
     * @param inputTypes     the input types
     * @param projectIndices the project indices
     */
    public OmniFilterAndProjectOperatorFactory(String expression, VecType[] inputTypes, int[] projectIndices) {
        super(new Context(expression, inputTypes, projectIndices));
    }

    private static native long createFilterAndProjectOperatorFactory(String inputTypes, int inputLength,
            String expression, int[] projectIndices, int projectLength);

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createFilterAndProjectOperatorFactory(VecTypeSerializer.serialize(context.inputTypes),
                context.inputTypes.length, context.expression, context.projectIndices, context.projectIndices.length);
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] inputTypes;

        private final String expression;

        private final int[] projectIndices;

        /**
         * Instantiates a new Context.
         *
         * @param expression     the expression
         * @param inputTypes     the input types
         * @param projectIndices the project indices
         */
        public Context(String expression, VecType[] inputTypes, int[] projectIndices) {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expression = requireNonNull(expression, "Expression is null.");
            this.projectIndices = requireNonNull(projectIndices, "Project indices is null.");
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression, Arrays.hashCode(inputTypes), Arrays.hashCode(projectIndices));
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
            return Objects.equals(expression, that.expression) && Arrays.equals(inputTypes, that.inputTypes)
                    && Arrays.equals(projectIndices, that.projectIndices);
        }
    }
}
