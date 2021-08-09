/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class DictionaryVecType extends VecType {
    /**
     * Dictionary singleton
     */
    public static final DictionaryVecType DICTIONARY = new DictionaryVecType();

    /**
     * The construct.
     */
    public DictionaryVecType() {
        super(VecTypeId.OMNI_VEC_TYPE_DICTIONARY);
    }
}
