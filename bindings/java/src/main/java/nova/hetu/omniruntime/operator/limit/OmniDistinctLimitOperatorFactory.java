/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.limit;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.OmniJitContext;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The type Omni distinct limit operator factory.
 *
 * @since 2021-06-30
 */
public class OmniDistinctLimitOperatorFactory
        extends OmniOperatorFactory<OmniDistinctLimitOperatorFactory.FactoryContext> {
    /**
     * Instantiates a new Omni distinct limit operator factory.
     *
     * @param sourceTypes the data types of each column
     * @param distinctCols the column index
     * @param hashCol col index of precomputed hash values
     * @param limit the limit count
     * @param isJitEnabled whether the jit is enabled
     */
    public OmniDistinctLimitOperatorFactory(DataType[] sourceTypes, int[] distinctCols, int hashCol, long limit,
            boolean isJitEnabled) {
        super(new FactoryContext(new JitContext(sourceTypes, distinctCols, hashCol, limit), isJitEnabled));
    }

    /**
     * Instantiates a new Omni distinct limit operator factory with jit default.
     *
     * @param sourceTypes the data types of each column
     * @param distinctCols the column index
     * @param hashCol col index of precomputed hash values
     * @param limit the limit count
     */
    public OmniDistinctLimitOperatorFactory(DataType[] sourceTypes, int[] distinctCols, int hashCol, long limit) {
        this(sourceTypes, distinctCols, hashCol, limit, true);
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
     * @since 2021-06-30
     */
    public static class JitContext implements OmniJitContext {
        private static Set<DataType.DataTypeId> supportTypes = new HashSet<DataType.DataTypeId>() {
            {
                add(DataType.DataTypeId.OMNI_INT);
                add(DataType.DataTypeId.OMNI_LONG);
                add(DataType.DataTypeId.OMNI_DOUBLE);
                add(DataType.DataTypeId.OMNI_BOOLEAN);
                add(DataType.DataTypeId.OMNI_DECIMAL64);
                add(DataType.DataTypeId.OMNI_DECIMAL128);
                add(DataType.DataTypeId.OMNI_DATE32);
                add(DataType.DataTypeId.OMNI_CHAR);
                add(DataType.DataTypeId.OMNI_VARCHAR);
            }
        };

        private DataType[] sourceTypes;
        private int[] distinctCols;
        private int hashCol;
        private long limit;

        /**
         * Instantiates a new Context.
         *
         * @param sourceTypes the data types of each column
         * @param distinctCols the column index
         * @param hashCol col index of precomputed hash values
         * @param limit the limit count
         */
        public JitContext(DataType[] sourceTypes, int[] distinctCols, int hashCol, long limit) {
            this.sourceTypes = requireNonNull(sourceTypes, "Source types array is null.");
            this.distinctCols = requireNonNull(distinctCols, "Distinct cols array is null.");
            checkDataType();
            this.limit = limit;
            this.hashCol = hashCol;
        }

        private void checkDataType() {
            for (int index : distinctCols) {
                if (!supportTypes.contains(sourceTypes[index].getId())) {
                    throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT,
                            "DataType(" + sourceTypes[index].getId() + ") of column" + index + " is not supported.");
                }
            }
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
     * @since 2021-06-30
     */
    public static class FactoryContext extends OmniOperatorFactoryContext<JitContext> {
        /**
         * Instantiates a new Context.
         *
         * @param jitContext the jit context
         * @param isJitEnabled whether the jit is enabled
         */
        public FactoryContext(JitContext jitContext, boolean isJitEnabled) {
            super(jitContext, isJitEnabled);
        }

        @Override
        protected long createNativeJitContext(JitContext context) {
            // future.
            return 0;
        }
    }
}
