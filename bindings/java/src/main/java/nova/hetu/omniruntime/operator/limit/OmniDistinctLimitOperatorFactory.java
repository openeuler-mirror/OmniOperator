/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The type Omni distinct limit operator factory.
 *
 * @since 20210630
 */
public class OmniDistinctLimitOperatorFactory
        extends
            OmniOperatorFactory<OmniDistinctLimitOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni distinct limit operator factory.
     *
     * @param sourceTypes the data types of each column
     * @param distinctCols the column index
     * @param hashCol col index of precomputed hash values
     * @param limit the limit count
     */
    public OmniDistinctLimitOperatorFactory(DataType[] sourceTypes, int[] distinctCols, int hashCol, long limit) {
        super(new FactoryContext(new JitContext(sourceTypes, distinctCols, hashCol, limit)));
    }

    private static native long createDistinctLimitOperatorFactory(String sourceTypes, int[] distinctCols, int hashCol,
            long limit);

    @Override
    protected long createNativeOperatorFactory(FactoryContext factoryContext) {
        JitContext context = factoryContext.getJitContext();
        return createDistinctLimitOperatorFactory(DataTypeSerializer.serialize(context.sourceTypes),
                context.distinctCols, context.hashCol, context.limit);
    }

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class JitContext implements OmniJitContext {
        private DataType[] sourceTypes;
        private int[] distinctCols;
        private int hashCol;
        private long limit;

        public JitContext(DataType[] sourceTypes, int[] distinctCols, int hashCol, long limit) {
            this.sourceTypes = requireNonNull(sourceTypes, "Source types array is null.");
            this.distinctCols = requireNonNull(distinctCols, "Distinct cols array is null.");
            this.limit = limit;
            this.hashCol = hashCol;
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
            return this.limit == context.limit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceTypes), Arrays.hashCode(distinctCols), this.limit, this.hashCol);
        }
    }

    /**
     * The type Factory context.
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
            // future.
            return 0;
        }
    }
}
