#pragma once

#include <cstdint>
#include <unordered_map>
#include <mutex>
#include "../operator/op_template.h"

template<typename T>
class OpTemplateCache
{
    public:
    void put(uint64_t operatorId, T op) {
        mtx.lock();
        opTemplateMap.insert(std::pair<uint64_t, T>(operatorId, op));
        mtx.unlock();
    }
        
    void remove(uint64_t operatorId) {
        mtx.lock();
        if (opTemplateMap.find(operatorId) != opTemplateMap.end()) {
            auto oper = opTemplateMap[operatorId];
            delete oper;
        }
        opTemplateMap.erase(operatorId);
        mtx.unlock();
    }
    
    T get(uint64_t operatorId) {
        T op = nullptr;
        mtx.lock();
        if (opTemplateMap.find(operatorId) != opTemplateMap.end())
        {
            /* code */
            op = opTemplateMap.at(operatorId);
            mtx.unlock();
            return op;
        }
        mtx.unlock();
        return op;
    }
    
    private:
    std::mutex mtx;
    std::unordered_map<uint64_t, T> opTemplateMap;
};


