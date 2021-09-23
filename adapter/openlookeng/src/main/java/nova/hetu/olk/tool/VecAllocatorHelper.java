/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import java.util.Map;

import io.prestosql.operator.TaskContext;
import io.prestosql.spi.block.Block;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;

public class VecAllocatorHelper
{
    private static final String VECTOR_ALLOCATOR_PROPERTY_NAME = "vector_allocator";
    private static final String EXTENSION_TASK_ID_PROPERTY_NAME = "extension_column_reader_task_id";

    public static void setVectorAllocatorToTaskContext(TaskContext taskContext, VecAllocator vecAllocator)
    {
        taskContext.getTaskExtendProperties().put(VECTOR_ALLOCATOR_PROPERTY_NAME, vecAllocator);
    }

    public static VecAllocator getVecAllocatorFromTaskContext(TaskContext taskContext) {
        VecAllocator vecAllocator = (VecAllocator) taskContext.getTaskExtendProperties().get(VECTOR_ALLOCATOR_PROPERTY_NAME);
        return vecAllocator;
    }

    public static VecAllocator getVecAllocatorFromExtensionProperties(Map<String, String> extensionColumnReadersProperties) {
        String scope = extensionColumnReadersProperties.get(EXTENSION_TASK_ID_PROPERTY_NAME);
        if (scope == null) {
            return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
        }
        return VecAllocatorFactory.get(scope);
    }

    public static VecAllocator getVecAllocatorFromBlocks(Block[] blocks)
    {
        for (Block block : blocks) {
            if (block.isExtensionBlock()) {
                return  ((Vec) block.getValues()).getAllocator();
            }
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }
}
