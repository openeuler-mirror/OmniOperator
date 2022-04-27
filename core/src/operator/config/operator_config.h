/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit iterator
 */
#ifndef OMNI_RUNTIME_OPERATOR_CONFIG_H
#define OMNI_RUNTIME_OPERATOR_CONFIG_H

#include <cstdint>
#include <string>
#include <nlohmann/json.hpp>
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
    SparkSpillConfig(bool enabled, const std::string &spillPath, uint64_t maxSpillBytes, int32_t numElementsThreshold)
        : SpillConfig(SPILL_CONFIG_SPARK, enabled, spillPath, maxSpillBytes),
          numElementsForSpillThreshold(numElementsThreshold)
    {}

    SparkSpillConfig(const std::string &spillPath, uint64_t maxSpillBytes, int32_t numElementsThreshold)
        : SparkSpillConfig(true, spillPath, maxSpillBytes, numElementsThreshold)
    {}

    SparkSpillConfig(const std::string &spillPath, int32_t numElementsThreshold)
        : SparkSpillConfig(spillPath, DEFAULT_MAX_SPILL_BYTES, numElementsThreshold)
    {}

    SparkSpillConfig(const SparkSpillConfig &spillConfig)
        : SpillConfig((SpillConfig &)spillConfig),
          numElementsForSpillThreshold(spillConfig.numElementsForSpillThreshold)
    {}

    ~SparkSpillConfig() override = default;

    bool NeedSpill(MemoryBuilder *memoryBuilder) override;

    int32_t GetSpillThreshold() const
    {
        return numElementsForSpillThreshold;
    }

private:
    int32_t numElementsForSpillThreshold;
};

class OperatorConfig {
public:
    OperatorConfig() : spillConfig(new SpillConfig()) {}

    OperatorConfig(const OperatorConfig &operatorConfig);

    explicit OperatorConfig(SpillConfig *spillConfig)
        : spillConfig((spillConfig != nullptr) ? spillConfig : new SpillConfig())
    {}

    explicit OperatorConfig(const SpillConfig &spillConfig) : spillConfig(new SpillConfig(spillConfig)) {}

    explicit OperatorConfig(const SparkSpillConfig &sparkSpillConfig)
        : spillConfig(new SparkSpillConfig(sparkSpillConfig))
    {}

    ~OperatorConfig()
    {
        delete spillConfig;
    }

    SpillConfig *GetSpillConfig() const
    {
        return spillConfig;
    }

    void SetSpillConfig(SpillConfig *pSpillConfig)
    {
        this->spillConfig = pSpillConfig;
    }

    static OperatorConfig DeserializeOperatorConfig(const std::string &configString);

    static void CheckOperatorConfig(const OperatorConfig &operatorConfig);

private:
    SpillConfig *spillConfig;
};
}
}

#endif // OMNI_RUNTIME_OPERATOR_CONFIG_H
