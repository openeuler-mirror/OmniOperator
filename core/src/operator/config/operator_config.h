/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit iterator
 */
#ifndef OMNI_RUNTIME_OPERATOR_CONFIG_H
#define OMNI_RUNTIME_OPERATOR_CONFIG_H

#include <cstdint>
#include <string>
#include <nlohmann/json.hpp>
#include "memory/memory_manager.h"
#include "operator/memory_builder.h"

namespace omniruntime {
namespace op {
enum SpillConfigId {
    SPILL_CONFIG_NONE = 0,
    SPILL_CONFIG_OLK = 1,
    SPILL_CONFIG_SPARK = 2,
    SPILL_CONFIG_INVALID
};

NLOHMANN_JSON_SERIALIZE_ENUM(SpillConfigId, { { SPILL_CONFIG_NONE, "SPILL_CONFIG_NONE" },
    { SPILL_CONFIG_OLK, "SPILL_CONFIG_OLK" },
    { SPILL_CONFIG_SPARK, "SPILL_CONFIG_SPARK" },
    { SPILL_CONFIG_INVALID, "SPILL_CONFIG_INVALID" } })

class SpillConfig {
public:
    SpillConfig() : SpillConfig(SPILL_CONFIG_NONE, false, "", DEFAULT_MAX_SPILL_BYTES) {}

    SpillConfig(SpillConfigId id, bool enabled, const std::string &spillPath, uint64_t maxSpillBytes)
        : spillConfigId(id), spillEnabled(enabled), spillPath(spillPath), maxSpillBytes(maxSpillBytes)
    {}

    SpillConfig(const SpillConfig &spillConfig)
        : SpillConfig(spillConfig.spillConfigId, spillConfig.spillEnabled, spillConfig.spillPath,
        spillConfig.maxSpillBytes)
    {}

    virtual ~SpillConfig() = default;

    virtual bool NeedSpill(MemoryBuilder *memoryBuilder)
    {
        return false;
    }

    SpillConfigId GetSpillConfigId()
    {
        return spillConfigId;
    }

    bool IsSpillEnabled() const
    {
        return spillEnabled;
    }

    std::string &GetSpillPath()
    {
        return spillPath;
    }

    uint64_t GetMaxSpillBytes() const
    {
        return maxSpillBytes;
    }

    void SetMaxSpillBytes(uint64_t maxSpillSize)
    {
        this->maxSpillBytes = maxSpillSize;
    }

protected:
    static constexpr uint64_t DEFAULT_MAX_SPILL_BYTES = 100UL * (1 << 30);
    SpillConfigId spillConfigId;
    bool spillEnabled;
    std::string spillPath;
    uint64_t maxSpillBytes;
};

class OLKSpillConfig : public SpillConfig {
public:
    OLKSpillConfig() : SpillConfig(SPILL_CONFIG_OLK, false, "", UINT64_MAX) {}

    OLKSpillConfig(bool spillEnabled, const std::string &spillPath, uint64_t maxSpillBytes)
        : SpillConfig(SPILL_CONFIG_OLK, spillEnabled, spillPath, maxSpillBytes)
    {}

    OLKSpillConfig(OLKSpillConfig &olkSpillConfig) : SpillConfig(static_cast<SpillConfig &>(olkSpillConfig)) {}

    ~OLKSpillConfig() override = default;
};

class SparkSpillConfig : public SpillConfig {
public:
    SparkSpillConfig(bool enabled, const std::string &spillPath, uint64_t maxSpillBytes, int32_t numElementsThreshold,
        int32_t memUsagePctThreshold)
        : SpillConfig(SPILL_CONFIG_SPARK, enabled, spillPath, maxSpillBytes),
          numElementsForSpillThreshold(numElementsThreshold)
    {
        auto limit = mem::MemoryManager::GetGlobalMemoryLimit();
        if (limit == mem::MemoryManager::UNLIMIT) {
            memUsageForSpillThreshold = INT64_MAX;
        } else {
            memUsageForSpillThreshold = limit * memUsagePctThreshold / 100;
        }
    }

    SparkSpillConfig(const std::string &spillPath, uint64_t maxSpillBytes, int32_t numElementsThreshold,
        int32_t memUsageThreshold = 90)
        : SparkSpillConfig(true, spillPath, maxSpillBytes, numElementsThreshold, memUsageThreshold)
    {}

    SparkSpillConfig(const std::string &spillPath, int32_t numElementsThreshold)
        : SparkSpillConfig(spillPath, DEFAULT_MAX_SPILL_BYTES, numElementsThreshold)
    {}

    SparkSpillConfig(const SparkSpillConfig &spillConfig)
        : SpillConfig((SpillConfig &)spillConfig),
          numElementsForSpillThreshold(spillConfig.numElementsForSpillThreshold),
          memUsageForSpillThreshold(spillConfig.memUsageForSpillThreshold)
    {}

    ~SparkSpillConfig() override = default;

    bool NeedSpill(MemoryBuilder *memoryBuilder) override;

    int32_t GetSpillRowThreshold() const
    {
        return numElementsForSpillThreshold;
    }

    int64_t GetSpillMemThreshold() const
    {
        return memUsageForSpillThreshold;
    }

private:
    int32_t numElementsForSpillThreshold;
    int64_t memUsageForSpillThreshold;
};


enum OverflowConfigId {
    OVERFLOW_CONFIG_EXCEPTION = 0,
    OVERFLOW_CONFIG_NULL = 1
};

NLOHMANN_JSON_SERIALIZE_ENUM(OverflowConfigId,
    { { OVERFLOW_CONFIG_EXCEPTION, "OVERFLOW_CONFIG_EXCEPTION" }, { OVERFLOW_CONFIG_NULL, "OVERFLOW_CONFIG_NULL" } })
class OverflowConfig {
public:
    OverflowConfig() : OverflowConfig(OVERFLOW_CONFIG_EXCEPTION) {}

    OverflowConfig(OverflowConfigId overflowConfigId) : overflowConfigId(overflowConfigId) {}

    OverflowConfigId GetOverflowConfigId() const
    {
        return overflowConfigId;
    }

    bool IsOverflowAsNull() const
    {
        return overflowConfigId == OVERFLOW_CONFIG_NULL;
    }

private:
    OverflowConfigId overflowConfigId;
};

class OperatorConfig {
public:
    OperatorConfig()
        : spillConfig(new SpillConfig()),
          overflowConfig(new OverflowConfig()),
          isSkipVerify(false),
          adaptivityThreshold(-1)
    {}

    OperatorConfig(const OperatorConfig &operatorConfig);

    OperatorConfig(SpillConfig *spillConfig, OverflowConfig *overflowConfig, bool isSkipVerify,
        int adaptivityThreshold = -1)
        : spillConfig((spillConfig != nullptr) ? spillConfig : new SpillConfig()),
          overflowConfig((overflowConfig != nullptr) ? overflowConfig : new OverflowConfig()),
          isSkipVerify(isSkipVerify),
          adaptivityThreshold(adaptivityThreshold)
    {}

    OperatorConfig(SpillConfig *spillConfig, OverflowConfig *overflowConfig)
        : spillConfig((spillConfig != nullptr) ? spillConfig : new SpillConfig()),
          overflowConfig((overflowConfig != nullptr) ? overflowConfig : new OverflowConfig()),
          isSkipVerify(false)
    {}

    explicit OperatorConfig(const OverflowConfig &overflowConfig)
        : spillConfig(new SpillConfig()), overflowConfig(new OverflowConfig(overflowConfig)), isSkipVerify(false)
    {}

    explicit OperatorConfig(const SpillConfig &spillConfig)
        : spillConfig(new SpillConfig(spillConfig)), overflowConfig(new OverflowConfig()), isSkipVerify(false)
    {}

    explicit OperatorConfig(const SparkSpillConfig &sparkSpillConfig)
        : spillConfig(new SparkSpillConfig(sparkSpillConfig)), overflowConfig(new OverflowConfig()), isSkipVerify(false)
    {}

    ~OperatorConfig()
    {
        delete spillConfig;
        delete overflowConfig;
    }

    SpillConfig *GetSpillConfig() const
    {
        return spillConfig;
    }

    void SetSpillConfig(SpillConfig *pSpillConfig)
    {
        this->spillConfig = pSpillConfig;
    }

    OverflowConfig *GetOverflowConfig() const
    {
        return overflowConfig;
    }

    void SetOverflowConfig(OverflowConfig *pOverflowConfig)
    {
        this->overflowConfig = pOverflowConfig;
    }

    bool IsSkipVerify() const
    {
        return isSkipVerify;
    }

    void SetSkipVerify(bool needSkipVerify)
    {
        this->isSkipVerify = needSkipVerify;
    }

    int GetAdaptivityThreshold() const
    {
        return adaptivityThreshold;
    }
    static OperatorConfig DeserializeOperatorConfig(const std::string &configString);

    static void CheckOperatorConfig(const OperatorConfig &operatorConfig);

private:
    SpillConfig *spillConfig = nullptr;
    OverflowConfig *overflowConfig = nullptr;
    bool isSkipVerify = false;
    int adaptivityThreshold = -1;
};
}
}

#endif // OMNI_RUNTIME_OPERATOR_CONFIG_H
