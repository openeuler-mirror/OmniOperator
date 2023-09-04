/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit iterator
 */
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>
#include "util/error_code.h"
#include "operator/spill/spill_tracker.h"
#include "util/omni_exception.h"
#include "memory/memory_manager.h"
#include "operator_config.h"

namespace omniruntime {
namespace op {
constexpr int32_t GB_UNIT = (1 << 30);
constexpr double DEFAULT_AVAILABLE_THRESHOLD = 0.9;

bool SparkSpillConfig::NeedSpill(MemoryBuilder *memoryBuilder)
{
    auto usedMemorySize = mem::MemoryManager::GetGlobalAccountedMemory();
    if (IsSpillEnabled() &&
        (memoryBuilder->GetRowCount() >= GetSpillRowThreshold() || usedMemorySize >= GetSpillMemThreshold())) {
        LogDebug("spill get row count %d, row threshold %d, and get memory usage %lld, memory threshold %lld.",
            memoryBuilder->GetRowCount(), GetSpillRowThreshold(), usedMemorySize, GetSpillMemThreshold());
        return true;
    } else {
        return false;
    }
}

OperatorConfig::OperatorConfig(const OperatorConfig &operatorConfig)
{
    auto inputSpillConfig = operatorConfig.GetSpillConfig();
    auto configId = inputSpillConfig->GetSpillConfigId();
    switch (configId) {
        case SPILL_CONFIG_NONE:
        case SPILL_CONFIG_OLK:
        case SPILL_CONFIG_INVALID: {
            spillConfig = new SpillConfig(*inputSpillConfig);
            break;
        }
        case SPILL_CONFIG_SPARK: {
            auto sparkSpillConfig = static_cast<SparkSpillConfig *>(inputSpillConfig);
            spillConfig = new SparkSpillConfig(*sparkSpillConfig);
            break;
        }
    }
}

OperatorConfig OperatorConfig::DeserializeOperatorConfig(const std::string &configString)
{
    SpillConfig *resultSpillConfig = nullptr;
    OverflowConfig *resultOverflowConfig = nullptr;
    bool needSkipVerify = false;

    auto result = nlohmann::json::parse(configString);
    if (result.contains("overflowConfig")) {
        auto overflowConfigId = result.at("overflowConfig").at("overflowConfigId").get<OverflowConfigId>();
        resultOverflowConfig = new OverflowConfig(overflowConfigId);
    }

    if (result.contains("spillConfig")) {
        auto spillConfigId = result.at("spillConfig").at("spillConfigId").get<SpillConfigId>();
        auto spillEnabled = result.at("spillConfig").at("spillEnabled").get<bool>();
        auto spillPath = result.at("spillConfig").at("spillPath").get<std::string>();
        auto maxSpillBytes = result.at("spillConfig").at("maxSpillBytes").get<uint64_t>();

        switch (spillConfigId) {
            case SPILL_CONFIG_NONE:
            case SPILL_CONFIG_OLK:
            case SPILL_CONFIG_INVALID: {
                resultSpillConfig = new SpillConfig(spillConfigId, spillEnabled, spillPath, maxSpillBytes);
                break;
            }
            case SPILL_CONFIG_SPARK: {
                auto numElementsForSpillThreshold =
                    result.at("spillConfig").at("numElementsForSpillThreshold").get<int32_t>();
                auto memUsagePctForSpillThreshold =
                    result.at("spillConfig").at("memUsagePctForSpillThreshold").get<int32_t>();
                resultSpillConfig = new SparkSpillConfig(spillEnabled, spillPath, maxSpillBytes,
                    numElementsForSpillThreshold, memUsagePctForSpillThreshold);
                break;
            }
            default: {
                std::string omniExceptionInfo = "In fucntion DeserializeOperatorConfig, no such data type " +
                    std::to_string(static_cast<int>(spillConfigId));
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    if (result.contains("skipExpressionVerify")) {
        needSkipVerify = result.at("skipExpressionVerify").get<bool>();
    }

    return OperatorConfig{ resultSpillConfig, resultOverflowConfig, needSkipVerify };
}

void CheckHasEnoughDiskSpace(const char *spillPathChars, SpillConfig &spillConfig)
{
    struct statfs diskInfo;
    auto result = statfs(spillPathChars, &diskInfo);
    if (result != 0) {
        rmdir(spillPathChars);
        throw exception::OmniException(GetErrorCode(ErrorCode::DISK_STAT_FAILED),
            GetErrorMessage(ErrorCode::DISK_STAT_FAILED));
    }

    auto availableDiskSize =
        static_cast<uint64_t>(static_cast<double>(diskInfo.f_bavail * diskInfo.f_bsize) * DEFAULT_AVAILABLE_THRESHOLD);
    auto maxSpillBytes = spillConfig.GetMaxSpillBytes();
    if (availableDiskSize < maxSpillBytes) {
        spillConfig.SetMaxSpillBytes(availableDiskSize);
        std::string message = GetErrorMessage(ErrorCode::DISK_SPACE_NOT_ENOUGH) +
            "The disk available size:" + std::to_string(availableDiskSize / GB_UNIT) +
            "GB and the max spill size:" + std::to_string(maxSpillBytes / GB_UNIT) + "GB.";
        LogWarn("%s", message.c_str());
    }
}

static void CreateSpillDirectory(const char *spillPathChars)
{
    auto result = mkdir(spillPathChars, S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    if (result != 0) {
        throw exception::OmniException(GetErrorCode(ErrorCode::MKDIR_FAILED), GetErrorMessage(ErrorCode::MKDIR_FAILED));
    }
}

static void CreateSpillDirectories(const char *spillPathChars)
{
    int32_t i = 0;
    while (spillPathChars[i] != '\0') {
        if (spillPathChars[i] == '/' || spillPathChars[i] == '\\') {
            if (access(spillPathChars, 0) == -1) {
                CreateSpillDirectory(spillPathChars);
            }
        }
        i++;
    }
}

void OperatorConfig::CheckOperatorConfig(const OperatorConfig &operatorConfig)
{
    auto inputSpillConfig = operatorConfig.GetSpillConfig();
    auto spillEnabled = inputSpillConfig->IsSpillEnabled();
    if (!spillEnabled) {
        return;
    }

    auto &spillPath = inputSpillConfig->GetSpillPath();
    if (spillPath.empty()) {
        // enable spill but the spill path is invalid
        throw exception::OmniException(GetErrorCode(ErrorCode::EMPTY_PATH), GetErrorMessage(ErrorCode::EMPTY_PATH));
    }

    auto &rootSpillTracker = GetRootSpillTracker();
    if (rootSpillTracker.IsSpillPathPresent(spillPath)) {
        return;
    }

    auto spillPathChars = spillPath.c_str();
    if (access(spillPathChars, 0) == 0) {
        // the path has existed
        throw exception::OmniException(GetErrorCode(ErrorCode::PATH_EXIST), GetErrorMessage(ErrorCode::PATH_EXIST));
    }

    // create directory
    CreateSpillDirectories(spillPathChars);

    CheckHasEnoughDiskSpace(spillPathChars, *inputSpillConfig);

    InitRootSpillTracker(spillPath, inputSpillConfig->GetMaxSpillBytes());
}
}
}
