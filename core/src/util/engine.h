/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Header
 */

#ifndef OMNI_RUNTIME_ENGINE_H
#define OMNI_RUNTIME_ENGINE_H
#include "global_log.h"

using EngineType = enum class EngineType {
    OLK = 0,
    Spark,
    None
};

class EngineUtil {
private:
    EngineUtil() : engineType(EngineType::None) {}

public:
    EngineUtil(const EngineUtil &) = delete;
    EngineUtil(const EngineUtil &&) = delete;
    EngineUtil &operator = (const EngineUtil &) = delete;
    static EngineUtil &GetInstance()
    {
        static EngineUtil engineUtil;
        return engineUtil;
    }

    void SetEngineType(char *typeStr)
    {
        if (typeStr == nullptr) {
            Log("Did not set engine type", LogType::LOG_INFO);
            return;
        }
        std::string str(typeStr);
        if (str == "OLK") {
            engineType = EngineType::OLK;
            return;
        }
        if (str == "Spark") {
            engineType = EngineType::Spark;
            return;
        }
        std::string err = "Not supported engine type " + str;
        Log(err, LogType::LOG_INFO);
    }

    EngineType GetEngineType()
    {
        return engineType;
    }

private:
    EngineType engineType;
};
#endif // OMNI_RUNTIME_ENGINE_H
