/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.project;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The type Omni project operator factory.
 *
 * @since 20210630
 */
public class OmniProjectOperatorFactory extends OmniOperatorFactory<OmniProjectOperatorFactory.Context> {
    /**
     * Instantiates a new Omni project operator factory.
     *
     * @param expressions the expressions
     * @param inputTypes  the input types
     */
    public OmniProjectOperatorFactory(String[] expressions, VecType[] inputTypes) {
        super(new Context(expressions, inputTypes));
    }

    private static native long createProjectOperatorFactory(String inputTypes, int inputLength, Object[] expressions,
            int expressionsLength);

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createProjectOperatorFactory(VecTypeSerializer.serialize(context.inputTypes), context.inputTypes.length,
                context.expressions, context.expressions.length);
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] inputTypes;

        private final String[] expressions;

        /**
         * Instantiates a new Context.
         *
         * @param expressions the expressions
         * @param inputTypes  the input types
         */
        public Context(String[] expressions, VecType[] inputTypes) {
            this.inputTypes = requireNonNull(inputTypes, "Input types array is null.");
            this.expressions = requireNonNull(expressions, "Expressions is null.");
        }

        @Override
        public int hashCode() {
            return Objects.hash(expressions, inputTypes);
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
            return Objects.equals(expressions, that.expressions) && Objects.equals(inputTypes, that.inputTypes);
        }
    }
}
