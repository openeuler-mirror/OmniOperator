/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import java.util.HashMap;
import java.util.Map;

public class VecAllocatorFactory {

    public static Map<String, VecAllocator> vecAllocators = new HashMap<>();

    synchronized public static VecAllocator create(String scope, CallBack createCallback) {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator == null) {
            allocator = new VecAllocator(scope);
            vecAllocators.put(scope, allocator);
            if (createCallback != null) {
                createCallback.callback();
            }
        }
        return allocator;
    }

    synchronized public static VecAllocator get(String scope) {
        if (vecAllocators.containsKey(scope)) {
            return vecAllocators.get(scope);
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }

    synchronized public static void delete(String scope) {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator != null) {
            allocator.close();
            vecAllocators.remove(scope);
        }
    }

    public interface CallBack {
        void callback();
    }
}
