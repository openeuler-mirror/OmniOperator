/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.StructDataType;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_STRUCT;

/**
 * struct vec.
 *
 * @since 2025-8-29
 */
public class StructVec extends ComplexVec {

  public StructVec(StructDataType type, int size) {
    this(newComplexVectorNative(size, OMNI_ENCODING_STRUCT.ordinal(), type.getFieldTypes()), type, size);
  }

  public StructVec(long nativeVector, StructDataType type, int size) {
    super(nativeVector, getComplexCapacityNative(nativeVector, OMNI_ENCODING_STRUCT.ordinal()), size, type);
  }

  @Override
  public Vec slice(int start, int length) {
    return null;
  }

  @Override
  public Vec copyPositions(int[] positions, int offset, int length) {
    return null;
  }

  @Override
  public int getRealValueBufCapacityInBytes() {
    return 0;
  }

}
