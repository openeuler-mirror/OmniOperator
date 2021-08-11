/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator.union;

import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * The type Omni union operator factory.
 *
 * @since 20210630
 */
public class OmniUnionOperatorFactory extends OmniOperatorFactory<OmniUnionOperatorFactory.Context> {
    /**
     * Instantiates a new Omni union operator factory.
     *
     * @param sourceTypes the source type
     * @param isDistinct mark union or union all
     */
    public OmniUnionOperatorFactory(VecType[] sourceTypes, boolean isDistinct) {
        super(new Context(sourceTypes, isDistinct));
    }

    @Override
    protected long createNativeOperatorFactory(Context context) {
        return createUnionOperatorFactory(VecTypeSerializer.serialize(context.sourceTypes), context.isDistinct);
    }

    private static native long createUnionOperatorFactory(String sourceTypes, boolean isDistinct);

    /**
     * The type Context.
     *
     * @since 20210630
     */
    public static class Context extends OmniOperatorFactoryContext {
        private final VecType[] sourceTypes;
        private final boolean isDistinct;

        public Context(VecType[] sourceTypes, boolean isDistinct) {
            this.sourceTypes = sourceTypes;
            this.isDistinct = isDistinct;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Context context = null;
            if ( obj instanceof Context) {
                context = (Context) obj;
            }
            return isDistinct == context.isDistinct &&
                    Arrays.equals(sourceTypes, context.sourceTypes);
        }

        @Override
        public int hashCode() {
            return  Objects.hash(Arrays.hashCode(sourceTypes), isDistinct);
        }
    }
}
