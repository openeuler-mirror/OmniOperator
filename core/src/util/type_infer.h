#ifndef __TYPE_INFER_H__
#define __TYPE_INFER_H__

#include <stdint.h>

template<typename T>
class TypeUtil {
public:
    static T* cast(void* val) {
        return reinterpret_cast<T*>(val);
    }
    static T* cast(void* val, uint32_t offset) {
        return reinterpret_cast<T*>(val) + offset;
    }
};

#endif