/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import java.util.HashMap;
import java.util.Map;

/**
 * vec allocator factory.
 *
 * @since 2021-09-23
 */
public class VecAllocatorFactory {
    private static Map<String, VecAllocator> vecAllocators = new HashMap<>();

    /**
     * create the vector allocator with specified scope and call back.
     *
     * @param scope the specified scope
     * @param createCallback the call back
     * @return vector allocator
     */
    public static synchronized VecAllocator create(String scope, CallBack createCallback) {
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

    /**
     * get the vector allocator by specified scope.
     *
     * @param scope the scope for vector
     * @return vector allocator
     */
    public static synchronized VecAllocator get(String scope) {
        if (vecAllocators.containsKey(scope)) {
            return vecAllocators.get(scope);
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }

    /**
     * delete the vector allocator by specified scope.
     *
     * @param scope the scope for vector
     */
    public static synchronized void delete(String scope) {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator != null) {
            allocator.close();
            vecAllocators.remove(scope);
        }
    }

    /**
     * the call back interface.
     *
     * @since 2021-09-23
     */
    public interface CallBack {
        /**
         * the call back method.
         */
        void callback();
    }
}
